/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <unordered_map>
#include <unordered_set>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "system/sm_meta.h"

namespace {

std::unique_ptr<char[]> build_index_key(const IndexMeta& index, const RmRecord& record) {
    auto key = std::make_unique<char[]>(index.col_tot_len);
    int offset = 0;
    for (const auto& col : index.cols) {
        memcpy(key.get() + offset, record.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void append_context_line(Context* context, const std::string& line) {
    if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr) {
        return;
    }
    memcpy(context->data_send_ + *context->offset_, line.c_str(), line.size());
    *context->offset_ += static_cast<int>(line.size());
}

std::string format_index_columns(const std::vector<ColMeta>& cols) {
    std::string result = "(";
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i > 0) {
            result += ",";
        }
        result += cols[i].name;
    }
    result += ")";
    return result;
}

IxIndexHandle* get_index_handle(SmManager* sm_manager, const std::string& tab_name, const IndexMeta& index) {
    auto index_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
    return sm_manager->ihs_.at(index_name).get();
}

int64_t rid_to_token(const Rid& rid) {
    return (static_cast<int64_t>(rid.page_no) << 32) | static_cast<uint32_t>(rid.slot_no);
}

void ensure_unique_key_available(IxIndexHandle* ih, const char* key, Transaction* txn,
                                 const std::unordered_set<int64_t>& ignored_rids) {
    std::vector<Rid> result;
    if (!ih->get_value(key, &result, txn)) {
        return;
    }
    for (const auto& rid : result) {
        int64_t rid_token = rid_to_token(rid);
        if (ignored_rids.count(rid_token) == 0) {
            throw RMDBError("Duplicate key for unique index");
        }
    }
}

void delete_index_entries(SmManager* sm_manager, const std::string& tab_name, const TabMeta& tab, const RmRecord& record,
                          Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = build_index_key(index, record);
        get_index_handle(sm_manager, tab_name, index)->delete_entry(key.get(), txn);
    }
}

void insert_index_entries(SmManager* sm_manager, const std::string& tab_name, const TabMeta& tab, const RmRecord& record,
                          const Rid& rid, Transaction* txn) {
    for (const auto& index : tab.indexes) {
        auto key = build_index_key(index, record);
        get_index_handle(sm_manager, tab_name, index)->insert_entry(key.get(), rid, txn);
    }
}

struct UpdatePlan {
    Rid rid;
    RmRecord old_record;
    RmRecord new_record;
    std::vector<std::unique_ptr<char[]>> old_keys;
    std::vector<std::unique_ptr<char[]>> new_keys;

    UpdatePlan(UpdatePlan&&) = default;
    UpdatePlan& operator=(UpdatePlan&&) = default;
    UpdatePlan(const UpdatePlan&) = delete;
    UpdatePlan& operator=(const UpdatePlan&) = delete;
};

UpdatePlan build_update_plan(const std::string& tab_name, const TabMeta& tab, RmFileHandle* fh, const Rid& rid,
                             const RmRecord& new_record, Context* context) {
    auto old_record = fh->get_record(rid, context);
    if (old_record == nullptr) {
        throw RMDBError("Record not found");
    }

    UpdatePlan plan{rid, *old_record, new_record, {}, {}};
    plan.old_keys.reserve(tab.indexes.size());
    plan.new_keys.reserve(tab.indexes.size());
    for (const auto& index : tab.indexes) {
        plan.old_keys.push_back(build_index_key(index, plan.old_record));
        plan.new_keys.push_back(build_index_key(index, plan.new_record));
    }
    return plan;
}

}  // namespace

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir (const std::string& db_name) {
    struct stat st;
    return stat (db_name.c_str (), &st) == 0 && S_ISDIR (st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db (const std::string& db_name) {
    if (is_dir (db_name)) {
        throw DatabaseExistsError (db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system (cmd.c_str ()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError ();
    }
    if (chdir (db_name.c_str ()) < 0) {  // 进入名为db_name的目录
        throw UnixError ();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta ();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs (DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file (LOG_FILE_NAME);

    // 回到根目录
    if (chdir ("..") < 0) {
        throw UnixError ();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db (const std::string& db_name) {
    if (!is_dir (db_name)) {
        throw DatabaseNotFoundError (db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system (cmd.c_str ()) < 0) {
        throw UnixError ();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db (const std::string& db_name) {
    if (!is_dir (db_name)) {
        throw DatabaseNotFoundError (db_name);
    }

    if (chdir (db_name.c_str ()) < 0) {
        throw UnixError ();
    }

    std::ifstream ifs (DB_META_NAME);
    ifs >> db_;
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        fhs_.emplace (tab.name, rm_manager_->open_file (tab.name));
    }

    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        for (auto& index : tab.indexes) {
            auto index_name = ix_manager_->get_index_name (tab.name, index.cols);
            ihs_.emplace (index_name, ix_manager_->open_index (tab.name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta () {
    // 默认清空文件
    std::ofstream ofs (DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db () {
    for (auto& entry : fhs_) {
        rm_manager_->close_file (entry.second.get ());
    }
    fhs_.clear ();
    for (auto& entry : ihs_) {
        ix_manager_->close_index (entry.second.get ());
    }
    ihs_.clear ();
    flush_meta ();
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables (Context* context) {
    std::fstream outfile;
    outfile.open ("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer (1);
    printer.print_separator (context);
    printer.print_record ({"Tables"}, context);
    printer.print_separator (context);
    for (auto& entry : db_.tabs_) {
        auto& tab = entry.second;
        printer.print_record ({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator (context);
    outfile.close ();
}

void SmManager::show_index (const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table (tab_name);
    std::fstream outfile;
    outfile.open ("output.txt", std::ios::out | std::ios::app);
    for (const auto& index : tab.indexes) {
        std::string line = "| " + tab.name + " | unique | " + format_index_columns (index.cols) + " |\n";
        append_context_line (context, line);
        outfile << line;
    }
    outfile.close ();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table (const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table (tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer (captions.size ());
    // Print header
    printer.print_separator (context);
    printer.print_record (captions, context);
    printer.print_separator (context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str (col.type), col.index ? "YES" : "NO"};
        printer.print_record (field_info, context);
    }
    // Print footer
    printer.print_separator (context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table (const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table (tab_name)) {
        throw TableExistsError (tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back (col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file (tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace (tab_name, rm_manager_->open_file (tab_name));

    flush_meta ();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table (const std::string& tab_name, Context* context) {
    if (!db_.is_table (tab_name)) {
        throw TableNotFoundError (tab_name);
    }
    TabMeta& tab = db_.get_table (tab_name);

    std::vector<std::vector<std::string>> index_names;
    index_names.reserve (tab.indexes.size ());
    for (const auto& index : tab.indexes) {
        std::vector<std::string> names;
        names.reserve (index.cols.size ());
        for (const auto& col : index.cols) {
            names.push_back (col.name);
        }
        index_names.push_back (std::move (names));
    }
    for (const auto& names : index_names) {
        drop_index (tab_name, names, context);
    }
    if (fhs_.find (tab_name) != fhs_.end ()) {
        rm_manager_->close_file (fhs_[tab_name].get ());
        fhs_[tab_name].reset ();
        fhs_.erase (tab_name);
    }

    rm_manager_->destroy_file (tab_name);

    db_.tabs_.erase (tab_name);

    flush_meta ();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index (const std::string& tab_name, const std::vector<std::string>& col_names,
                              Context* context) {
    if (!db_.is_table (tab_name)) {
        throw TableNotFoundError (tab_name);
    }

    TabMeta& tab = db_.get_table (tab_name);
    if (tab.is_index (col_names)) {
        throw IndexExistsError (tab_name, col_names);
    }

    std::vector<ColMeta> index_cols;
    index_cols.reserve (col_names.size ());
    int col_tot_len = 0;
    for (const auto& col_name : col_names) {
        auto col_it = tab.get_col (col_name);
        index_cols.push_back (*col_it);
        col_tot_len += col_it->len;
    }

    ix_manager_->create_index (tab_name, index_cols);
    auto ih = ix_manager_->open_index (tab_name, index_cols);
    auto* fh = fhs_.at (tab_name).get ();

    try {
        RmScan scan (fh);
        while (!scan.is_end ()) {
            auto rid = scan.rid ();
            auto record = fh->get_record (rid, context);
            auto key = build_index_key ({tab_name, col_tot_len, static_cast<int> (index_cols.size ()), index_cols},
                                        *record);
            std::vector<Rid> result;
            if (ih->get_value (key.get (), &result, context ? context->txn_ : nullptr) && !result.empty ()) {
                throw RMDBError ("Duplicate key when creating index");
            }
            ih->insert_entry (key.get (), rid, context ? context->txn_ : nullptr);
            scan.next ();
        }
    } catch (...) {
        ix_manager_->close_index (ih.get ());
        ix_manager_->destroy_index (tab_name, index_cols);
        throw;
    }

    IndexMeta meta = {.tab_name = tab_name,
                      .col_tot_len = col_tot_len,
                      .col_num = static_cast<int> (index_cols.size ()),
                      .cols = index_cols};
    tab.indexes.push_back (meta);

    auto index_name = ix_manager_->get_index_name (tab_name, index_cols);
    ihs_[index_name] = std::move (ih);
    flush_meta ();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index (const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table (tab_name)) {
        throw TableNotFoundError (tab_name);
    }
    TabMeta& tab = db_.get_table (tab_name);
    auto index_it = tab.get_index_meta (col_names);
    auto index_name = ix_manager_->get_index_name (tab_name, index_it->cols);
    if (ihs_.count (index_name) != 0) {
        ix_manager_->close_index (ihs_.at (index_name).get ());
        ihs_.erase (index_name);
    }
    ix_manager_->destroy_index (tab_name, index_it->cols);
    tab.indexes.erase (index_it);
    flush_meta ();
}

void SmManager::drop_index (const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context) {
    std::vector<std::string> names;
    names.reserve (col_names.size ());
    for (const auto& col : col_names) {
        names.push_back (col.name);
    }
    drop_index (tab_name, names, context);
}

Rid SmManager::insert_tuple(const std::string& tab_name, const RmRecord& record, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();

    std::vector<std::unique_ptr<char[]>> index_keys;
    index_keys.reserve(tab.indexes.size());
    static const std::unordered_set<int64_t> ignored_rids;
    for (const auto& index : tab.indexes) {
        auto key = build_index_key(index, record);
        ensure_unique_key_available(get_index_handle(this, tab_name, index), key.get(),
                                    context ? context->txn_ : nullptr, ignored_rids);
        index_keys.push_back(std::move(key));
    }

    Rid rid = fh->insert_record(record.data, context);
    if (context != nullptr && context->txn_ != nullptr) {
        context->txn_->append_write_record(new WriteRecord(WType::INSERT_TUPLE, tab_name, rid));
    }

    for (size_t i = 0; i < tab.indexes.size(); ++i) {
        get_index_handle(this, tab_name, tab.indexes[i])->insert_entry(index_keys[i].get(), rid,
                                                                       context ? context->txn_ : nullptr);
    }
    return rid;
}

void SmManager::delete_tuple(const std::string& tab_name, const Rid& rid, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();
    auto record = fh->get_record(rid, context);
    if (record == nullptr) {
        return;
    }

    if (context != nullptr && context->txn_ != nullptr) {
        context->txn_->append_write_record(new WriteRecord(WType::DELETE_TUPLE, tab_name, rid, *record));
    }

    delete_index_entries(this, tab_name, tab, *record, context ? context->txn_ : nullptr);
    fh->delete_record(rid, context);
}

void SmManager::update_tuple(const std::string& tab_name, const Rid& rid, const RmRecord& record, Context* context) {
    update_tuples(tab_name, {{rid, record}}, context);
}

void SmManager::update_tuples(const std::string& tab_name, const std::vector<std::pair<Rid, RmRecord>>& records,
                              Context* context) {
    if (records.empty()) {
        return;
    }

    TabMeta& tab = db_.get_table(tab_name);
    auto* fh = fhs_.at(tab_name).get();

    std::vector<UpdatePlan> plans;
    plans.reserve(records.size());

    std::unordered_set<int64_t> target_rids;
    target_rids.reserve(records.size());
    for (const auto& [rid, record] : records) {
        int64_t rid_token = rid_to_token(rid);
        target_rids.insert(rid_token);
        plans.push_back(build_update_plan(tab_name, tab, fh, rid, record, context));
    }

    for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
        const auto& index = tab.indexes[index_no];
        auto* ih = get_index_handle(this, tab_name, index);
        std::unordered_map<std::string, int64_t> final_keys;
        final_keys.reserve(plans.size());

        for (const auto& plan : plans) {
            std::string final_key(plan.new_keys[index_no].get(), index.col_tot_len);
            int64_t rid_token = rid_to_token(plan.rid);
            auto [it, inserted] = final_keys.emplace(final_key, rid_token);
            if (!inserted && it->second != rid_token) {
                throw RMDBError("Duplicate key for unique index");
            }
        }

        for (const auto& plan : plans) {
            ensure_unique_key_available(ih, plan.new_keys[index_no].get(), context ? context->txn_ : nullptr,
                                        target_rids);
        }
    }

    for (auto& plan : plans) {
        if (context != nullptr && context->txn_ != nullptr) {
            context->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name, plan.rid, plan.old_record));
        }

        for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
            const auto& index = tab.indexes[index_no];
            if (memcmp(plan.old_keys[index_no].get(), plan.new_keys[index_no].get(), index.col_tot_len) == 0) {
                continue;
            }
            get_index_handle(this, tab_name, index)->delete_entry(plan.old_keys[index_no].get(),
                                                                  context ? context->txn_ : nullptr);
        }

        fh->update_record(plan.rid, plan.new_record.data, context);

        for (size_t index_no = 0; index_no < tab.indexes.size(); ++index_no) {
            const auto& index = tab.indexes[index_no];
            if (memcmp(plan.old_keys[index_no].get(), plan.new_keys[index_no].get(), index.col_tot_len) == 0) {
                continue;
            }
            get_index_handle(this, tab_name, index)->insert_entry(plan.new_keys[index_no].get(), plan.rid,
                                                                  context ? context->txn_ : nullptr);
        }
    }
}

void SmManager::rollback_write(WriteRecord* write_record, Transaction* txn) {
    if (write_record == nullptr) {
        return;
    }

    const auto& tab_name = write_record->GetTableName();
    auto fh_it = fhs_.find(tab_name);
    if (fh_it == fhs_.end()) {
        return;
    }

    auto* fh = fh_it->second.get();
    auto& tab = db_.get_table(tab_name);
    auto& rid = write_record->GetRid();
    auto& record = write_record->GetRecord();

    switch (write_record->GetWriteType()) {
        case WType::INSERT_TUPLE: {
            auto inserted_record = fh->get_record(rid, nullptr);
            if (inserted_record != nullptr) {
                delete_index_entries(this, tab_name, tab, *inserted_record, txn);
                fh->delete_record(rid, nullptr);
            }
            break;
        }
        case WType::DELETE_TUPLE: {
            fh->insert_record(rid, record.data);
            insert_index_entries(this, tab_name, tab, record, rid, txn);
            break;
        }
        case WType::UPDATE_TUPLE: {
            auto current_record = fh->get_record(rid, nullptr);
            if (current_record != nullptr) {
                delete_index_entries(this, tab_name, tab, *current_record, txn);
            }
            fh->update_record(rid, record.data, nullptr);
            insert_index_entries(this, tab_name, tab, record, rid, txn);
            break;
        }
    }
}

