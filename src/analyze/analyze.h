/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/common.h"
#include "parser/parser.h"
#include "system/sm.h"

class Query {
    public:
    struct JoinTreeNode {
        std::string table_name;
        JoinType join_type = INNER_JOIN;
        std::shared_ptr<JoinTreeNode> left;
        std::shared_ptr<JoinTreeNode> right;
        std::vector<std::string> tables;
        std::vector<Condition> join_conds;

        bool is_leaf () const {
            return left == nullptr && right == nullptr;
        }
    };

    std::shared_ptr<ast::TreeNode> parse;
    std::shared_ptr<JoinTreeNode> jointree;
    bool is_explain = false;
    bool select_star = false;
    // where条件
    std::vector<Condition> conds;
    // 投影列
    std::vector<TabCol> cols;
    // 表名
    std::vector<std::string> tables;
    std::unordered_map<std::string, std::string> table_sources;
    std::unordered_map<std::string, std::string> table_aliases;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    // insert 的values值
    std::vector<Value> values;

    Query () {
    }
};

class Analyze {
    private:
    SmManager *sm_manager_;

    public:
    Analyze (SmManager *sm_manager) : sm_manager_ (sm_manager) {
    }
    ~Analyze () {
    }

    std::shared_ptr<Query> do_analyze (std::shared_ptr<ast::TreeNode> root);

    private:
    TabCol check_column (const std::vector<ColMeta> &all_cols, TabCol target,
                         const std::unordered_map<std::string, std::string> &visible_to_table = {}) const;
    void get_all_cols (const std::vector<std::string> &tab_names,
                       const std::unordered_map<std::string, std::string> &table_sources,
                       std::vector<ColMeta> &all_cols) const;
    void get_clause (const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) const;
    void check_clause (const std::vector<std::string> &tab_names, std::vector<Condition> &conds,
                       const std::unordered_map<std::string, std::string> &visible_to_table = {},
                       const std::unordered_map<std::string, std::string> &table_sources = {}) const;
    void collect_table_refs (const std::shared_ptr<ast::TableRef> &table_ref,
                             std::vector<std::string> &tab_names,
                             std::unordered_map<std::string, std::string> &visible_to_table,
                             std::unordered_map<std::string, std::string> &table_sources) const;
    std::shared_ptr<Query::JoinTreeNode> build_join_tree (
        const std::shared_ptr<ast::TableRef> &table_ref,
        const std::unordered_map<std::string, std::string> &visible_to_table) const;
    Value convert_sv_value (const std::shared_ptr<ast::Value> &sv_val) const;
    CompOp convert_sv_comp_op (ast::SvCompOp op) const;
};
