/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"

#include <memory>
#include <vector>

#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

namespace {

void delete_write_records(const std::shared_ptr<std::deque<WriteRecord *>> &write_set) {
    if (write_set == nullptr) {
        return;
    }
    for (auto *write_record : *write_set) {
        delete write_record;
    }
    write_set->clear();
}

void release_locks(Transaction *txn, LockManager *lock_manager) {
    if (txn == nullptr || lock_manager == nullptr) {
        return;
    }
    auto lock_set = txn->get_lock_set();
    if (lock_set == nullptr) {
        return;
    }
    std::vector<LockDataId> locks(lock_set->begin(), lock_set->end());
    for (const auto &lock_data_id : locks) {
        lock_manager->unlock(txn, lock_data_id);
    }
    lock_set->clear();
}

void clear_txn_resources(Transaction *txn) {
    if (txn == nullptr) {
        return;
    }
    if (auto index_latch_page_set = txn->get_index_latch_page_set(); index_latch_page_set != nullptr) {
        index_latch_page_set->clear();
    }
    if (auto index_deleted_page_set = txn->get_index_deleted_page_set(); index_deleted_page_set != nullptr) {
        index_deleted_page_set->clear();
    }
}

std::unique_ptr<char[]> make_index_key(const IndexMeta &index, const RmRecord &record) {
    auto key = std::make_unique<char[]>(index.col_tot_len);
    int offset = 0;
    for (size_t i = 0; i < index.col_num; ++i) {
        memcpy(key.get() + offset, record.data + index.cols[i].offset, index.cols[i].len);
        offset += index.cols[i].len;
    }
    return key;
}

void delete_index_entry(SmManager *sm_manager, const std::string &tab_name, const RmRecord &record, Transaction *txn) {
    auto &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager->ihs_.at(index_name).get();
        auto key = make_index_key(index, record);
        ih->delete_entry(key.get(), txn);
    }
}

void insert_index_entry(SmManager *sm_manager, const std::string &tab_name, const RmRecord &record, const Rid &rid,
                        Transaction *txn) {
    auto &tab = sm_manager->db_.get_table(tab_name);
    for (auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager->ihs_.at(index_name).get();
        auto key = make_index_key(index, record);
        ih->insert_entry(key.get(), rid, txn);
    }
}

}  // namespace

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin (Transaction* txn, LogManager* log_manager) {
    std::lock_guard<std::mutex> guard(latch_);

    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }

    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_++);
    TransactionManager::txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit (Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }

    delete_write_records(txn->get_write_set());
    release_locks(txn, lock_manager_);
    clear_txn_resources(txn);

    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort (Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }

    auto write_set = txn->get_write_set();
    if (write_set != nullptr) {
        for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
            auto *write_record = *it;
            auto fh_it = sm_manager_->fhs_.find(write_record->GetTableName());
            if (fh_it == sm_manager_->fhs_.end()) {
                continue;
            }

            auto *fh = fh_it->second.get();
            auto &rid = write_record->GetRid();
            auto &record = write_record->GetRecord();

            switch (write_record->GetWriteType()) {
                case WType::INSERT_TUPLE: {
                    auto inserted_record = fh->get_record(rid, nullptr);
                    if (inserted_record != nullptr) {
                        delete_index_entry(sm_manager_, write_record->GetTableName(), *inserted_record, txn);
                        fh->delete_record(rid, nullptr);
                    }
                    break;
                }
                case WType::DELETE_TUPLE: {
                    fh->insert_record(rid, record.data);
                    insert_index_entry(sm_manager_, write_record->GetTableName(), record, rid, txn);
                    break;
                }
                case WType::UPDATE_TUPLE: {
                    auto current_record = fh->get_record(rid, nullptr);
                    if (current_record != nullptr) {
                        delete_index_entry(sm_manager_, write_record->GetTableName(), *current_record, txn);
                    }
                    fh->update_record(rid, record.data, nullptr);
                    insert_index_entry(sm_manager_, write_record->GetTableName(), record, rid, txn);
                    break;
                }
            }
        }
    }

    delete_write_records(write_set);
    release_locks(txn, lock_manager_);
    clear_txn_resources(txn);

    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    txn->set_state(TransactionState::ABORTED);
}
