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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
    private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    public:
    UpdateExecutor (SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                    std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table (tab_name);
        fh_ = sm_manager_->fhs_.at (tab_name).get ();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next () override {
        for (auto &rid : rids_) {
            auto record = fh_->get_record (rid, context_);

            std::vector<std::unique_ptr<char[]>> old_keys;
            std::vector<std::unique_ptr<char[]>> new_keys;
            old_keys.reserve (tab_.indexes.size ());
            new_keys.reserve (tab_.indexes.size ());

            for (auto &index : tab_.indexes) {
                auto old_key = std::make_unique<char[]> (index.col_tot_len);
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy (old_key.get () + offset, record->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                old_keys.push_back (std::move (old_key));
            }

            auto updated_record = std::make_unique<RmRecord> (*record);
            for (auto &clause : set_clauses_) {
                auto col = tab_.get_col (clause.lhs.col_name);
                auto &val = clause.rhs;
                memcpy (updated_record->data + col->offset, val.raw->data, col->len);
            }

            for (auto &index : tab_.indexes) {
                auto ix_handle =
                    sm_manager_->ihs_.at (sm_manager_->get_ix_manager ()->get_index_name (tab_name_, index.cols))
                        .get ();
                auto new_key = std::make_unique<char[]> (index.col_tot_len);
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy (new_key.get () + offset, updated_record->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                std::vector<Rid> result;
                if (memcmp (new_key.get (), old_keys[new_keys.size ()].get (), index.col_tot_len) != 0 &&
                    ix_handle->get_value (new_key.get (), &result, context_->txn_) && !result.empty ()) {
                    bool has_other_rid = false;
                    for (const auto &existing_rid : result) {
                        if (existing_rid != rid) {
                            has_other_rid = true;
                            break;
                        }
                    }
                    if (has_other_rid) {
                        throw RMDBError ("Duplicate key for unique index");
                    }
                }
                new_keys.push_back (std::move (new_key));
            }

            for (size_t i = 0; i < tab_.indexes.size (); ++i) {
                auto &index = tab_.indexes[i];
                if (memcmp (old_keys[i].get (), new_keys[i].get (), index.col_tot_len) == 0) {
                    continue;
                }
                auto ix_handle =
                    sm_manager_->ihs_.at (sm_manager_->get_ix_manager ()->get_index_name (tab_name_, index.cols))
                        .get ();
                ix_handle->delete_entry (old_keys[i].get (), context_->txn_);
            }

            fh_->update_record (rid, updated_record->data, context_);

            for (size_t i = 0; i < tab_.indexes.size (); ++i) {
                auto &index = tab_.indexes[i];
                if (memcmp (old_keys[i].get (), new_keys[i].get (), index.col_tot_len) == 0) {
                    continue;
                }
                auto ix_handle =
                    sm_manager_->ihs_.at (sm_manager_->get_ix_manager ()->get_index_name (tab_name_, index.cols))
                        .get ();
                ix_handle->insert_entry (new_keys[i].get (), rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid () override {
        return _abstract_rid;
    }
};
