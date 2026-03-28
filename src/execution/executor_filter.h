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
#include "executor_abstract.h"

class FilterExecutor : public AbstractExecutor {
    private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<Condition> conds_;
    std::unique_ptr<RmRecord> current_tuple_;

    public:
    FilterExecutor (std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds) {
        prev_ = std::move (prev);
        conds_ = std::move (conds);
    }

    const std::vector<ColMeta> &cols () const override {
        return prev_->cols ();
    }

    size_t tupleLen () const override {
        return prev_->tupleLen ();
    }

    void beginTuple () override {
        prev_->beginTuple ();
        advance_to_match ();
    }

    void nextTuple () override {
        if (prev_->is_end ()) {
            current_tuple_.reset ();
            return;
        }
        prev_->nextTuple ();
        advance_to_match ();
    }

    bool is_end () const override {
        return current_tuple_ == nullptr;
    }

    std::unique_ptr<RmRecord> Next () override {
        if (current_tuple_ == nullptr) {
            return nullptr;
        }
        return std::make_unique<RmRecord> (*current_tuple_);
    }

    Rid &rid () override {
        return prev_->rid ();
    }

    private:
    void advance_to_match () {
        current_tuple_.reset ();
        while (!prev_->is_end ()) {
            auto tuple = prev_->Next ();
            if (tuple != nullptr && evaluate_conditions (prev_->cols (), tuple.get (), conds_)) {
                current_tuple_ = std::move (tuple);
                return;
            }
            prev_->nextTuple ();
        }
    }
};
