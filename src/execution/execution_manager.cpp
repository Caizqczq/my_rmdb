/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <unordered_map>

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

namespace {

void append_context_text (Context *context, const std::string &text) {
    if (context == nullptr || context->data_send_ == nullptr || context->offset_ == nullptr) {
        return;
    }
    memcpy (context->data_send_ + *context->offset_, text.c_str (), text.size ());
    *context->offset_ += static_cast<int> (text.size ());
}

void append_output_line (Context *context, std::fstream &outfile, const std::string &line) {
    append_context_text (context, line);
    append_context_text (context, "\n");
    outfile << line << "\n";
}

std::string visible_table_name (const std::string &tab_name,
                                const std::unordered_map<std::string, std::string> &table_aliases) {
    auto it = table_aliases.find (tab_name);
    return it == table_aliases.end () ? tab_name : it->second;
}

std::string format_tab_col (const TabCol &col, const std::unordered_map<std::string, std::string> &table_aliases) {
    return visible_table_name (col.tab_name, table_aliases) + "." + col.col_name;
}

std::string format_value (const Value &value) {
    std::ostringstream oss;
    switch (value.type) {
        case TYPE_INT:
            oss << value.int_val;
            break;
        case TYPE_FLOAT:
            oss << std::fixed << std::setprecision (6) << value.float_val;
            break;
        case TYPE_STRING:
            oss << "'" << value.str_val << "'";
            break;
        default:
            break;
    }
    return oss.str ();
}

std::string format_comp_op (CompOp op) {
    switch (op) {
        case OP_EQ:
            return "=";
        case OP_NE:
            return "<>";
        case OP_LT:
            return "<";
        case OP_GT:
            return ">";
        case OP_LE:
            return "<=";
        case OP_GE:
            return ">=";
        default:
            return "?";
    }
}

std::string format_condition (const Condition &cond,
                              const std::unordered_map<std::string, std::string> &table_aliases) {
    std::ostringstream oss;
    oss << format_tab_col (cond.lhs_col, table_aliases) << format_comp_op (cond.op);
    if (cond.is_rhs_val) {
        oss << format_value (cond.rhs_val);
    } else {
        oss << format_tab_col (cond.rhs_col, table_aliases);
    }
    return oss.str ();
}

template <typename T>
std::string join_strings (const std::vector<T> &items) {
    std::ostringstream oss;
    for (size_t i = 0; i < items.size (); ++i) {
        if (i != 0) {
            oss << ",";
        }
        oss << items[i];
    }
    return oss.str ();
}

std::vector<std::string> collect_plan_tables (const std::shared_ptr<Plan> &plan) {
    if (auto scan = std::dynamic_pointer_cast<ScanPlan> (plan)) {
        return {scan->base_tab_name_};
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan> (plan)) {
        auto left_tables = collect_plan_tables (join->left_);
        auto right_tables = collect_plan_tables (join->right_);
        left_tables.insert (left_tables.end (), right_tables.begin (), right_tables.end ());
        std::sort (left_tables.begin (), left_tables.end ());
        return left_tables;
    }
    if (auto projection = std::dynamic_pointer_cast<ProjectionPlan> (plan)) {
        return collect_plan_tables (projection->subplan_);
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan> (plan)) {
        return collect_plan_tables (filter->subplan_);
    }
    if (auto sort = std::dynamic_pointer_cast<SortPlan> (plan)) {
        return collect_plan_tables (sort->subplan_);
    }
    if (auto dml = std::dynamic_pointer_cast<DMLPlan> (plan)) {
        return collect_plan_tables (dml->subplan_);
    }
    if (auto explain = std::dynamic_pointer_cast<ExplainPlan> (plan)) {
        return collect_plan_tables (explain->subplan_);
    }
    return {};
}

bool plan_contains_table (const std::shared_ptr<Plan> &plan, const std::string &tab_name) {
    auto tables = collect_plan_tables (plan);
    return std::find (tables.begin (), tables.end (), tab_name) != tables.end ();
}

std::vector<std::string> format_conditions (const std::vector<Condition> &conds,
                                            const std::unordered_map<std::string, std::string> &table_aliases) {
    std::vector<std::string> result;
    result.reserve (conds.size ());
    for (const auto &cond : conds) {
        result.push_back (format_condition (cond, table_aliases));
    }
    return result;
}


void render_explain_plan (const std::shared_ptr<Plan> &plan, const ExplainPlan &explain_plan,
                          std::vector<std::string> &lines) {
    if (auto projection = std::dynamic_pointer_cast<ProjectionPlan> (plan)) {
        std::vector<std::string> cols;
        cols.reserve (projection->sel_cols_.size ());
        for (const auto &col : projection->sel_cols_) {
            cols.push_back (format_tab_col (col, explain_plan.table_aliases_));
        }
        std::sort (cols.begin (), cols.end ());
        lines.push_back ("Project(columns=[" + join_strings (cols) + "])");
        render_explain_plan (projection->subplan_, explain_plan, lines);
        return;
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan> (plan)) {
        auto conds = format_conditions (filter->conds_, explain_plan.table_aliases_);
        lines.push_back ("Filter(condition=[" + join_strings (conds) + "])");
        render_explain_plan (filter->subplan_, explain_plan, lines);
        return;
    }
    if (auto sort = std::dynamic_pointer_cast<SortPlan> (plan)) {
        lines.push_back ("Sort(column=[" + format_tab_col (sort->sel_col_, explain_plan.table_aliases_) +
                         "],desc=" + std::string (sort->is_desc_ ? "true" : "false") + ")");
        render_explain_plan (sort->subplan_, explain_plan, lines);
        return;
    }
    if (auto join = std::dynamic_pointer_cast<JoinPlan> (plan)) {
        auto tables = collect_plan_tables (plan);
        auto conds = format_conditions (join->conds_, explain_plan.table_aliases_);
        lines.push_back ("Join(tables=[" + join_strings (tables) + "],condition=[" + join_strings (conds) + "])");
        render_explain_plan (join->left_, explain_plan, lines);
        render_explain_plan (join->right_, explain_plan, lines);
        return;
    }
    if (auto scan = std::dynamic_pointer_cast<ScanPlan> (plan)) {
        lines.push_back ("Scan(table=" + scan->base_tab_name_ + ")");
    }
}

void explain_plan_to_lines (const ExplainPlan &explain_plan, std::vector<std::string> &lines) {
    auto dml = std::dynamic_pointer_cast<DMLPlan> (explain_plan.subplan_);
    if (dml == nullptr || dml->tag != T_select) {
        lines.push_back ("EXPLAIN only supports SELECT statements");
        return;
    }
    if (auto projection = std::dynamic_pointer_cast<ProjectionPlan> (dml->subplan_)) {
        if (explain_plan.select_star_) {
            lines.push_back ("Project(columns=[*])");
            render_explain_plan (projection->subplan_, explain_plan, lines);
            return;
        }
    }
    render_explain_plan (dml->subplan_, explain_plan, lines);
}

}  // namespace

const char *help_info =
    "Supported SQL syntax:\n"
    "  command ;\n"
    "command:\n"
    "  HELP\n"
    "  EXIT\n"
    "  SHOW TABLES\n"
    "  SHOW INDEX FROM table_name\n"
    "  DESC table_name\n"
    "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
    "  DROP TABLE table_name\n"
    "  CREATE INDEX table_name (column_name)\n"
    "  DROP INDEX table_name (column_name)\n"
    "  INSERT INTO table_name VALUES (value [, value ...])\n"
    "  DELETE FROM table_name [WHERE where_clause]\n"
    "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
    "  SELECT selector FROM from_item [WHERE where_clause] [ORDER BY column [ASC|DESC]]\n"
    "  EXPLAIN SELECT selector FROM from_item [WHERE where_clause] [ORDER BY column [ASC|DESC]]\n"
    "  SET {ENABLE_NESTLOOP | ENABLE_SORTMERGE} = {true | false}\n"
    "  TXN_BEGIN | TXN_COMMIT | TXN_ABORT | TXN_ROLLBACK\n"
    "type:\n"
    "  {INT | FLOAT | CHAR(n)}\n"
    "from_item:\n"
    "  table_name [alias]\n"
    "  table_name [AS alias]\n"
    "  from_item , table_name [alias]\n"
    "  from_item JOIN table_name [alias] ON where_clause\n"
    "where_clause:\n"
    "  condition [AND condition ...]\n"
    "condition:\n"
    "  column op {column | value}\n"
    "column:\n"
    "  [table_name.]column_name\n"
    "op:\n"
    "  {= | <> | < | > | <= | >=}\n"
    "selector:\n"
    "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query (std::shared_ptr<Plan> plan, Context *context) {
    if (auto x = std::dynamic_pointer_cast<DDLPlan> (plan)) {
        switch (x->tag) {
            case T_CreateTable: {
                sm_manager_->create_table (x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable: {
                sm_manager_->drop_table (x->tab_name_, context);
                break;
            }
            case T_CreateIndex: {
                sm_manager_->create_index (x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case T_DropIndex: {
                sm_manager_->drop_index (x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError ("Unexpected field type");
                break;
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility (std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan> (plan)) {
        switch (x->tag) {
            case T_Help: {
                memcpy (context->data_send_ + *(context->offset_), help_info, strlen (help_info));
                *(context->offset_) = strlen (help_info);
                break;
            }
            case T_ShowTable: {
                sm_manager_->show_tables (context);
                break;
            }
            case T_ShowIndex: {
                sm_manager_->show_index (x->tab_name_, context);
                break;
            }
            case T_DescTable: {
                sm_manager_->desc_table (x->tab_name_, context);
                break;
            }
            case T_Transaction_begin: {
                // 显示开启一个事务
                context->txn_->set_txn_mode (true);
                break;
            }
            case T_Transaction_commit: {
                context->txn_ = txn_mgr_->get_transaction (*txn_id);
                txn_mgr_->commit (context->txn_, context->log_mgr_);
                break;
            }
            case T_Transaction_rollback: {
                context->txn_ = txn_mgr_->get_transaction (*txn_id);
                txn_mgr_->abort (context->txn_, context->log_mgr_);
                break;
            }
            case T_Transaction_abort: {
                context->txn_ = txn_mgr_->get_transaction (*txn_id);
                txn_mgr_->abort (context->txn_, context->log_mgr_);
                break;
            }
            default:
                throw InternalError ("Unexpected field type");
                break;
        }

    } else if (auto x = std::dynamic_pointer_cast<ExplainPlan> (plan)) {
        std::vector<std::string> lines;
        explain_plan_to_lines (*x, lines);
        std::fstream outfile;
        outfile.open ("output.txt", std::ios::out | std::ios::app);
        for (const auto &line : lines) {
            append_output_line (context, outfile, line);
        }
        outfile.close ();
    } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan> (plan)) {
        switch (x->set_knob_type_) {
            case ast::SetKnobType::EnableNestLoop: {
                planner_->set_enable_nestedloop_join (x->bool_value_);
                break;
            }
            case ast::SetKnobType::EnableSortMerge: {
                planner_->set_enable_sortmerge_join (x->bool_value_);
                break;
            }
            default: {
                throw RMDBError ("Not implemented!\n");
                break;
            }
        }
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from (std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                             Context *context) {
    std::vector<std::string> captions;
    captions.reserve (sel_cols.size ());
    for (auto &sel_col : sel_cols) {
        captions.push_back (sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer (sel_cols.size ());
    rec_printer.print_separator (context);
    rec_printer.print_record (captions, context);
    rec_printer.print_separator (context);
    // print header into file
    std::fstream outfile;
    outfile.open ("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (int i = 0; i < captions.size (); ++i) {
        outfile << " " << captions[i] << " |";
    }
    outfile << "\n";

    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (executorTreeRoot->beginTuple (); !executorTreeRoot->is_end (); executorTreeRoot->nextTuple ()) {
        auto Tuple = executorTreeRoot->Next ();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols ()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string (*(int *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string (*(float *)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string ((char *)rec_buf, col.len);
                col_str.resize (strlen (col_str.c_str ()));
            }
            columns.push_back (col_str);
        }
        // print record into buffer
        rec_printer.print_record (columns, context);
        // print record into file
        outfile << "|";
        for (int i = 0; i < columns.size (); ++i) {
            outfile << " " << columns[i] << " |";
        }
        outfile << "\n";
        num_rec++;
    }
    outfile.close ();
    // Print footer into buffer
    rec_printer.print_separator (context);
    // Print record count into buffer
    RecordPrinter::print_record_count (num_rec, context);
}

// 执行DML语句
void QlManager::run_dml (std::unique_ptr<AbstractExecutor> exec) {
    exec->Next ();
}
