/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "index/ix.h"

namespace {

bool condition_matches_column (const Condition &cond, const std::string &tab_name, const std::string &col_name) {
    return cond.is_rhs_val && cond.lhs_col.tab_name == tab_name && cond.lhs_col.col_name == col_name;
}

bool better_lower_bound (const Value &candidate, bool inclusive, bool &has_lower, Value &current,
                         bool &current_inclusive, ColType type, int len) {
    if (!has_lower) {
        has_lower = true;
        current = candidate;
        current_inclusive = inclusive;
        return true;
    }
    auto candidate_copy = candidate;
    auto current_copy = current;
    if (!candidate_copy.raw) {
        candidate_copy.init_raw (len);
    }
    if (!current_copy.raw) {
        current_copy.init_raw (len);
    }
    int cmp = ix_compare (candidate_copy.raw->data, current_copy.raw->data, type, len);
    if (cmp > 0 || (cmp == 0 && !inclusive && current_inclusive)) {
        current = candidate;
        current_inclusive = inclusive;
        return true;
    }
    return false;
}

bool better_upper_bound (const Value &candidate, bool inclusive, bool &has_upper, Value &current,
                         bool &current_inclusive, ColType type, int len) {
    if (!has_upper) {
        has_upper = true;
        current = candidate;
        current_inclusive = inclusive;
        return true;
    }
    auto candidate_copy = candidate;
    auto current_copy = current;
    if (!candidate_copy.raw) {
        candidate_copy.init_raw (len);
    }
    if (!current_copy.raw) {
        current_copy.init_raw (len);
    }
    int cmp = ix_compare (candidate_copy.raw->data, current_copy.raw->data, type, len);
    if (cmp < 0 || (cmp == 0 && !inclusive && current_inclusive)) {
        current = candidate;
        current_inclusive = inclusive;
        return true;
    }
    return false;
}

struct IndexChoice {
    bool usable = false;
    std::vector<std::string> col_names;
    std::vector<IndexRange> ranges;
    int matched_cols = 0;
    int eq_prefix = 0;
};

bool is_better_choice (const IndexChoice &lhs, const IndexChoice &rhs) {
    if (!lhs.usable) {
        return false;
    }
    if (!rhs.usable) {
        return true;
    }
    if (lhs.matched_cols != rhs.matched_cols) {
        return lhs.matched_cols > rhs.matched_cols;
    }
    if (lhs.eq_prefix != rhs.eq_prefix) {
        return lhs.eq_prefix > rhs.eq_prefix;
    }
    return lhs.col_names.size () < rhs.col_names.size ();
}

bool is_local_condition (const Condition &cond) {
    return cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name;
}

bool is_join_condition (const Condition &cond) {
    return !cond.is_rhs_val && cond.lhs_col.tab_name != cond.rhs_col.tab_name;
}

void swap_condition_operands (Condition &cond) {
    assert (!cond.is_rhs_val);
    static const std::map<CompOp, CompOp> swap_op = {
        {OP_EQ, OP_EQ},
        {OP_NE, OP_NE},
        {OP_LT, OP_GT},
        {OP_GT, OP_LT},
        {OP_LE, OP_GE},
        {OP_GE, OP_LE},
    };
    std::swap (cond.lhs_col, cond.rhs_col);
    cond.op = swap_op.at (cond.op);
}

std::string encode_value (const Value &value) {
    std::ostringstream oss;
    oss << value.type << ':';
    switch (value.type) {
        case TYPE_INT:
            oss << value.int_val;
            break;
        case TYPE_FLOAT:
            oss << value.float_val;
            break;
        case TYPE_STRING:
            oss << value.str_val;
            break;
        default:
            break;
    }
    return oss.str ();
}

std::string build_condition_signature (const Condition &cond) {
    std::ostringstream oss;
    oss << cond.lhs_col.tab_name << '.' << cond.lhs_col.col_name << '#' << cond.op << '#';
    if (cond.is_rhs_val) {
        oss << "value#" << encode_value (cond.rhs_val);
    } else {
        oss << cond.rhs_col.tab_name << '.' << cond.rhs_col.col_name;
    }
    return oss.str ();
}

int predicate_rank (const Condition &cond) {
    if (cond.is_rhs_val) {
        if (cond.op == OP_EQ) {
            return 0;
        }
        if (cond.op == OP_GT || cond.op == OP_GE || cond.op == OP_LT || cond.op == OP_LE) {
            return 1;
        }
        return 2;
    }
    if (cond.op == OP_EQ) {
        return 3;
    }
    return 4;
}

bool condition_mentions_table (const Condition &cond, const std::string &tab_name) {
    if (cond.lhs_col.tab_name == tab_name) {
        return true;
    }
    return !cond.is_rhs_val && cond.rhs_col.tab_name == tab_name;
}

bool contains_table (const std::vector<std::string> &tables, const std::string &tab_name) {
    return std::find (tables.begin (), tables.end (), tab_name) != tables.end ();
}

std::vector<Condition> extract_table_conditions (std::vector<Condition> &conds, const std::string &tab_name) {
    std::vector<Condition> extracted;
    auto it = conds.begin ();
    while (it != conds.end ()) {
        if (is_local_condition (*it) && condition_mentions_table (*it, tab_name)) {
            extracted.emplace_back (std::move (*it));
            it = conds.erase (it);
        } else {
            ++it;
        }
    }
    return extracted;
}

std::vector<Condition> extract_join_conditions (std::vector<Condition> &conds, const std::vector<std::string> &left_tables,
                                                const std::vector<std::string> &right_tables) {
    std::vector<Condition> join_conds;
    auto it = conds.begin ();
    while (it != conds.end ()) {
        if (!is_join_condition (*it)) {
            ++it;
            continue;
        }

        bool lhs_in_left = contains_table (left_tables, it->lhs_col.tab_name);
        bool rhs_in_left = contains_table (left_tables, it->rhs_col.tab_name);
        bool lhs_in_right = contains_table (right_tables, it->lhs_col.tab_name);
        bool rhs_in_right = contains_table (right_tables, it->rhs_col.tab_name);
        if ((lhs_in_left && rhs_in_right) || (lhs_in_right && rhs_in_left)) {
            join_conds.push_back (*it);
            it = conds.erase (it);
        } else {
            ++it;
        }
    }
    return join_conds;
}

int attach_join_condition (Condition cond, const std::shared_ptr<Plan> &plan) {
    if (auto join = std::dynamic_pointer_cast<JoinPlan> (plan)) {
        int left_mask = attach_join_condition (cond, join->left_);
        if (left_mask == 3) {
            return 3;
        }
        int right_mask = attach_join_condition (cond, join->right_);
        if (right_mask == 3) {
            return 3;
        }
        if (left_mask == 0 || right_mask == 0) {
            return left_mask + right_mask;
        }
        if (left_mask == 2) {
            swap_condition_operands (cond);
        }
        join->conds_.push_back (std::move (cond));
        return 3;
    }
    if (auto projection = std::dynamic_pointer_cast<ProjectionPlan> (plan)) {
        return attach_join_condition (std::move (cond), projection->subplan_);
    }
    if (auto filter = std::dynamic_pointer_cast<FilterPlan> (plan)) {
        return attach_join_condition (std::move (cond), filter->subplan_);
    }
    if (auto sort = std::dynamic_pointer_cast<SortPlan> (plan)) {
        return attach_join_condition (std::move (cond), sort->subplan_);
    }
    if (auto scan = std::dynamic_pointer_cast<ScanPlan> (plan)) {
        if (scan->tab_name_ == cond.lhs_col.tab_name) {
            return 1;
        }
        if (!cond.is_rhs_val && scan->tab_name_ == cond.rhs_col.tab_name) {
            return 2;
        }
    }
    return 0;
}

TabCol resolve_column_from_tables (SmManager *sm_manager, const std::vector<std::string> &tables,
                                   const std::unordered_map<std::string, std::string> &table_sources, TabCol target) {
    std::vector<ColMeta> all_cols;
    for (const auto &tab_name : tables) {
        const auto source_it = table_sources.find (tab_name);
        const std::string &base_tab_name = source_it == table_sources.end () ? tab_name : source_it->second;
        const auto &cols = sm_manager->db_.get_table (base_tab_name).cols;
        for (auto col : cols) {
            col.tab_name = tab_name;
            all_cols.push_back (std::move (col));
        }
    }

    if (target.tab_name.empty ()) {
        std::string resolved_tab_name;
        for (const auto &col : all_cols) {
            if (col.name != target.col_name) {
                continue;
            }
            if (!resolved_tab_name.empty ()) {
                throw AmbiguousColumnError (target.col_name);
            }
            resolved_tab_name = col.tab_name;
        }
        if (resolved_tab_name.empty ()) {
            throw ColumnNotFoundError (target.col_name);
        }
        target.tab_name = resolved_tab_name;
        return target;
    }

    for (const auto &col : all_cols) {
        if (col.tab_name == target.tab_name && col.name == target.col_name) {
            return target;
        }
    }
    throw ColumnNotFoundError (target.tab_name + '.' + target.col_name);
}

void collect_jointree_join_conditions (const std::shared_ptr<Query::JoinTreeNode> &jointree,
                                       std::vector<Condition> &join_conds) {
    if (jointree == nullptr || jointree->is_leaf ()) {
        return;
    }
    join_conds.insert (join_conds.end (), jointree->join_conds.begin (), jointree->join_conds.end ());
    collect_jointree_join_conditions (jointree->left, join_conds);
    collect_jointree_join_conditions (jointree->right, join_conds);
}

std::vector<TabCol> sort_projection_cols (std::set<TabCol> cols) {
    std::vector<TabCol> ordered_cols (cols.begin (), cols.end ());
    std::sort (ordered_cols.begin (), ordered_cols.end (),
               [] (const TabCol &lhs, const TabCol &rhs) { return std::tie (lhs.tab_name, lhs.col_name) < std::tie (rhs.tab_name, rhs.col_name); });
    return ordered_cols;
}

std::unordered_map<std::string, std::vector<TabCol>> collect_leaf_projection_cols (
    const std::shared_ptr<Query> &query, const std::vector<Condition> &join_conds) {
    std::unordered_map<std::string, std::set<TabCol>> required_cols;
    for (const auto &col : query->cols) {
        required_cols[col.tab_name].insert (col);
    }
    for (const auto &cond : join_conds) {
        required_cols[cond.lhs_col.tab_name].insert (cond.lhs_col);
        if (!cond.is_rhs_val) {
            required_cols[cond.rhs_col.tab_name].insert (cond.rhs_col);
        }
    }
    if (auto select = std::dynamic_pointer_cast<ast::SelectStmt> (query->parse)) {
        if (select->order != nullptr) {
            required_cols[select->order->cols->tab_name].insert (
                TabCol{select->order->cols->tab_name, select->order->cols->col_name});
        }
    }

    std::unordered_map<std::string, std::vector<TabCol>> result;
    for (auto &[tab_name, cols] : required_cols) {
        result.emplace (tab_name, sort_projection_cols (std::move (cols)));
    }
    return result;
}

struct JoinForestNode {
    std::vector<std::string> tables;
    std::shared_ptr<Plan> plan;
    size_t width = 0;
    bool is_leaf = true;
};

}  // namespace

bool Planner::get_index_scan_info (std::string tab_name, const std::string &base_tab_name,
                                   const std::vector<Condition> &curr_conds,
                                   std::vector<std::string> &index_col_names, std::vector<IndexRange> &index_ranges) {
    index_col_names.clear ();
    index_ranges.clear ();

    TabMeta &tab = sm_manager_->db_.get_table (base_tab_name);
    if (curr_conds.empty () && !tab.indexes.empty ()) {
        for (const auto &col : tab.indexes.front ().cols) {
            index_col_names.push_back (col.name);
        }
        return true;
    }
    IndexChoice best_choice;

    for (const auto &index : tab.indexes) {
        IndexChoice choice;
        for (const auto &col : index.cols) {
            choice.col_names.push_back (col.name);
        }
        choice.ranges.reserve (index.col_num);

        for (const auto &col : index.cols) {
            IndexRange range;
            range.col_name = col.name;

            bool has_eq = false;
            Value eq_value;
            Value lower_value;
            Value upper_value;
            bool has_lower = false;
            bool has_upper = false;
            bool lower_inclusive = true;
            bool upper_inclusive = true;

            for (const auto &cond : curr_conds) {
                if (!condition_matches_column (cond, tab_name, col.name)) {
                    continue;
                }

                if (cond.op == OP_EQ) {
                    has_eq = true;
                    eq_value = cond.rhs_val;
                    break;
                }

                if (cond.op == OP_GT || cond.op == OP_GE) {
                    better_lower_bound (cond.rhs_val, cond.op == OP_GE, has_lower, lower_value, lower_inclusive,
                                        col.type, col.len);
                } else if (cond.op == OP_LT || cond.op == OP_LE) {
                    better_upper_bound (cond.rhs_val, cond.op == OP_LE, has_upper, upper_value, upper_inclusive,
                                        col.type, col.len);
                }
            }

            if (has_eq) {
                range.has_lower = true;
                range.lower_inclusive = true;
                range.lower_value = eq_value;
                range.has_upper = true;
                range.upper_inclusive = true;
                range.upper_value = eq_value;
                range.has_equality = true;
                choice.eq_prefix++;
                choice.matched_cols++;
                choice.ranges.push_back (range);
                continue;
            }

            if (has_lower || has_upper) {
                range.has_lower = has_lower;
                range.lower_inclusive = lower_inclusive;
                range.lower_value = lower_value;
                range.has_upper = has_upper;
                range.upper_inclusive = upper_inclusive;
                range.upper_value = upper_value;
                choice.matched_cols++;
                choice.ranges.push_back (range);
                break;
            }

            break;
        }

        choice.usable = choice.matched_cols > 0;
        if (is_better_choice (choice, best_choice)) {
            best_choice = std::move (choice);
        }
    }

    if (!best_choice.usable) {
        return false;
    }

    index_col_names = std::move (best_choice.col_names);
    index_ranges = std::move (best_choice.ranges);
    return true;
}

std::shared_ptr<Query> Planner::logical_optimization (std::shared_ptr<Query> query, Context *context) {
    std::vector<Condition> normalized_conds;
    normalized_conds.reserve (query->conds.size ());
    std::unordered_set<std::string> seen;
    for (auto cond : query->conds) {
        if (!cond.is_rhs_val) {
            auto lhs_key = std::make_pair (cond.lhs_col.tab_name, cond.lhs_col.col_name);
            auto rhs_key = std::make_pair (cond.rhs_col.tab_name, cond.rhs_col.col_name);
            if (rhs_key < lhs_key) {
                swap_condition_operands (cond);
            }
        }
        auto signature = build_condition_signature (cond);
        if (seen.insert (signature).second) {
            normalized_conds.push_back (std::move (cond));
        }
    }

    std::stable_sort (normalized_conds.begin (), normalized_conds.end (),
                      [] (const Condition &lhs, const Condition &rhs) {
                          int lhs_rank = predicate_rank (lhs);
                          int rhs_rank = predicate_rank (rhs);
                          if (lhs_rank != rhs_rank) {
                              return lhs_rank < rhs_rank;
                          }
                          return build_condition_signature (lhs) < build_condition_signature (rhs);
                      });
    query->conds = std::move (normalized_conds);

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization (std::shared_ptr<Query> query, Context *context) {
    std::shared_ptr<Plan> plan = make_one_rel (query);

    // 其他物理优化

    // 处理orderby
    plan = generate_sort_plan (query, std::move (plan));

    return plan;
}

std::shared_ptr<Plan> Planner::generate_table_access_plan (const std::string &tab_name, const std::string &base_tab_name,
                                                           std::vector<Condition> conds) {
    std::vector<std::string> index_col_names;
    std::vector<IndexRange> index_ranges;
    bool index_exist = get_index_scan_info (tab_name, base_tab_name, conds, index_col_names, index_ranges);
    if (!index_exist) {
        return std::make_shared<ScanPlan> (T_SeqScan, sm_manager_, tab_name, base_tab_name, std::move (conds),
                                           std::vector<std::string> (), std::vector<IndexRange> ());
    }
    return std::make_shared<ScanPlan> (T_IndexScan, sm_manager_, tab_name, base_tab_name, std::move (conds),
                                       std::move (index_col_names), std::move (index_ranges));
}

std::shared_ptr<Plan> Planner::build_table_plan (const std::string &tab_name, const std::string &base_tab_name,
                                                 std::vector<Condition> local_conds,
                                                 const std::vector<TabCol> &leaf_projection_cols,
                                                 bool need_leaf_projection) {
    auto filter_conds = local_conds;
    std::shared_ptr<Plan> plan = generate_table_access_plan (tab_name, base_tab_name, std::move (local_conds));
    if (!filter_conds.empty ()) {
        plan = std::make_shared<FilterPlan> (T_Filter, std::move (plan), std::move (filter_conds));
    }
    if (need_leaf_projection && !leaf_projection_cols.empty ()) {
        plan = std::make_shared<ProjectionPlan> (T_Projection, std::move (plan), leaf_projection_cols);
    }
    return plan;
}

PlanTag Planner::choose_join_plan_tag (const std::vector<Condition> &join_conds) const {
    bool all_equality = !join_conds.empty () &&
                        std::all_of (join_conds.begin (), join_conds.end (),
                                     [] (const Condition &cond) { return is_join_condition (cond) && cond.op == OP_EQ; });

    if (enable_nestedloop_join) {
        return T_NestLoop;
    }
    if (enable_sortmerge_join && all_equality) {
        return T_SortMerge;
    }
    if (enable_sortmerge_join) {
        throw RMDBError ("Sort merge join requires equality predicates");
    }
    throw RMDBError ("No join executor selected!");
}

std::shared_ptr<Plan> Planner::make_one_rel (std::shared_ptr<Query> query) {
    std::vector<Condition> remaining_conds = query->conds;
    std::vector<Condition> join_conds;
    collect_jointree_join_conditions (query->jointree, join_conds);

    std::vector<JoinForestNode> forest;
    forest.reserve (query->tables.size ());
    for (const auto &cond : remaining_conds) {
        if (is_join_condition (cond)) {
            join_conds.push_back (cond);
        }
    }
    auto leaf_projection_cols = collect_leaf_projection_cols (query, join_conds);
    bool need_leaf_projection = query->tables.size () > 1 && !query->select_star;

    for (const auto &tab_name : query->tables) {
        const auto source_it = query->table_sources.find (tab_name);
        const std::string &base_tab_name = source_it == query->table_sources.end () ? tab_name : source_it->second;
        auto local_conds = extract_table_conditions (remaining_conds, tab_name);
        auto projection_it = leaf_projection_cols.find (tab_name);
        const std::vector<TabCol> empty_cols;
        const auto &table_projection_cols = projection_it == leaf_projection_cols.end () ? empty_cols : projection_it->second;
        auto plan = build_table_plan (tab_name, base_tab_name, local_conds, table_projection_cols, need_leaf_projection);

        TabMeta &tab = sm_manager_->db_.get_table (base_tab_name);
        size_t width = need_leaf_projection && !table_projection_cols.empty () ? table_projection_cols.size () : tab.cols.size ();
        forest.push_back (JoinForestNode{{tab_name}, std::move (plan), width, true});
    }

    auto better_pair = [&forest] (size_t lhs_i, size_t lhs_j, size_t rhs_i, size_t rhs_j) {
        size_t lhs_width = forest[lhs_i].width + forest[lhs_j].width;
        size_t rhs_width = forest[rhs_i].width + forest[rhs_j].width;
        if (lhs_width != rhs_width) {
            return lhs_width < rhs_width;
        }
        auto lhs_key = forest[lhs_i].tables.front () + "|" + forest[lhs_j].tables.front ();
        auto rhs_key = forest[rhs_i].tables.front () + "|" + forest[rhs_j].tables.front ();
        return lhs_key < rhs_key;
    };

    while (forest.size () > 1) {
        bool found = false;
        bool found_connectable = false;
        size_t best_i = 0;
        size_t best_j = 1;
        for (size_t i = 0; i < forest.size (); ++i) {
            for (size_t j = i + 1; j < forest.size (); ++j) {
                bool connectable = false;
                for (const auto &cond : join_conds) {
                    bool lhs_in_i = contains_table (forest[i].tables, cond.lhs_col.tab_name);
                    bool rhs_in_i = contains_table (forest[i].tables, cond.rhs_col.tab_name);
                    bool lhs_in_j = contains_table (forest[j].tables, cond.lhs_col.tab_name);
                    bool rhs_in_j = contains_table (forest[j].tables, cond.rhs_col.tab_name);
                    if ((lhs_in_i && rhs_in_j) || (lhs_in_j && rhs_in_i)) {
                        connectable = true;
                        break;
                    }
                }
                if (!found || (connectable && !found_connectable) ||
                    (connectable == found_connectable && better_pair (i, j, best_i, best_j))) {
                    best_i = i;
                    best_j = j;
                    found = true;
                    found_connectable = connectable;
                }
            }
        }

        auto left = std::move (forest[best_i]);
        auto right = std::move (forest[best_j]);
        if ((left.is_leaf && !right.is_leaf) ||
            (left.is_leaf == right.is_leaf &&
             (left.width > right.width || (left.width == right.width && left.tables > right.tables)))) {
            std::swap (left, right);
        }
        auto pair_join_conds = extract_join_conditions (join_conds, left.tables, right.tables);
        auto plan = std::make_shared<JoinPlan> (choose_join_plan_tag (pair_join_conds), left.plan, right.plan,
                                                std::move (pair_join_conds));

        JoinForestNode merged;
        merged.plan = std::move (plan);
        merged.tables = left.tables;
        merged.tables.insert (merged.tables.end (), right.tables.begin (), right.tables.end ());
        std::sort (merged.tables.begin (), merged.tables.end ());
        merged.width = left.width + right.width;
        merged.is_leaf = false;

        forest.erase (forest.begin () + best_j);
        forest.erase (forest.begin () + best_i);
        forest.push_back (std::move (merged));
    }

    std::shared_ptr<Plan> plan = forest.front ().plan;
    for (const auto &cond : join_conds) {
        if (is_join_condition (cond)) {
            attach_join_condition (cond, plan);
        }
    }
    return plan;
}

std::shared_ptr<Plan> Planner::generate_sort_plan (std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt> (query->parse);
    if (!x->has_sort) {
        return plan;
    }
    TabCol order_col = {.tab_name = x->order->cols->tab_name, .col_name = x->order->cols->col_name};
    order_col = resolve_column_from_tables (sm_manager_, query->tables, query->table_sources, order_col);
    return std::make_shared<SortPlan> (T_Sort, std::move (plan), std::move (order_col),
                                       x->order->orderby_dir == ast::OrderBy_DESC);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan (std::shared_ptr<Query> query, Context *context) {
    // 逻辑优化
    query = logical_optimization (std::move (query), context);

    // 物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization (query, context);
    plannerRoot = std::make_shared<ProjectionPlan> (T_Projection, std::move (plannerRoot), std::move (sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner (std::shared_ptr<Query> query, Context *context) {
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable> (query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef> (field)) {
                ColDef col_def = {.name = sv_col_def->col_name,
                                  .type = interp_sv_type (sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.push_back (col_def);
            } else {
                throw InternalError ("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan> (T_CreateTable, x->tab_name, std::vector<std::string> (), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable> (query->parse)) {
        // drop table;
        plannerRoot =
            std::make_shared<DDLPlan> (T_DropTable, x->tab_name, std::vector<std::string> (), std::vector<ColDef> ());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex> (query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan> (T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef> ());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex> (query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan> (T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef> ());
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowIndex> (query->parse)) {
        plannerRoot = std::make_shared<OtherPlan> (T_ShowIndex, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt> (query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan> (T_Insert, std::shared_ptr<Plan> (), x->tab_name, query->values,
                                                 std::vector<Condition> (), std::vector<SetClause> ());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt> (query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors =
            build_table_plan (x->tab_name, x->tab_name, query->conds, std::vector<TabCol> (), false);

        plannerRoot = std::make_shared<DMLPlan> (T_Delete, table_scan_executors, x->tab_name, std::vector<Value> (),
                                                 query->conds, std::vector<SetClause> ());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt> (query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors =
            build_table_plan (x->tab_name, x->tab_name, query->conds, std::vector<TabCol> (), false);
        plannerRoot = std::make_shared<DMLPlan> (T_Update, table_scan_executors, x->tab_name, std::vector<Value> (),
                                                 query->conds, query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt> (query->parse)) {
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo> (x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan (std::move (query), context);
        plannerRoot = std::make_shared<DMLPlan> (T_select, projection, std::string (), std::vector<Value> (),
                                                 std::vector<Condition> (), std::vector<SetClause> ());
    } else {
        throw InternalError ("Unexpected AST root");
    }
    return plannerRoot;
}
