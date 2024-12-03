//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // Initialize the child executors
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
  is_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple *left_tuple;
  Tuple *right_tuple;
  /** According to the function InferJoinSchema, get the schema for the output tuple*/
  std::vector<Column> columns;
  for (const auto &column : left_executor_->GetOutputSchema().GetColumns()) {
    columns.emplace_back(column);
  }
  for (const auto &column : right_executor_->GetOutputSchema().GetColumns()) {
    columns.push_back(column);
  }
  Schema schema(columns);
  if ((plan_->GetJoinType() == JoinType::INNER)) {
    while (true) {
      if (left_index_ == left_tuples_.size()) {
        return false;
      }
      left_tuple = &left_tuples_[left_index_];
      if (right_index_ == right_tuples_.size()) {
        right_index_ = 0;
        left_index_++;
        continue;
      }
      right_tuple = &right_tuples_[right_index_];
      if (plan_->Predicate()
              .EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                            right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = {value, &schema};
        right_index_++;
        return true;
      }
      right_index_++;
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (true) {
      if (left_index_ == left_tuples_.size()) {
        return false;
      }
      left_tuple = &left_tuples_[left_index_];
      if (right_index_ == right_tuples_.size()) {
        if (!is_matched_) {
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            value.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            value.push_back(
                ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
          }
          *tuple = {value, &schema};
          right_index_ = 0;
          left_index_++;
          is_matched_ = false;
          return true;
        }
        right_index_ = 0;
        left_index_++;
        is_matched_ = false;
        continue;
      }
      right_tuple = &right_tuples_[right_index_];
      if (plan_->Predicate()
              .EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                            right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          value.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = {value, &schema};
        right_index_++;
        is_matched_ = true;
        return true;
      }
      right_index_++;
    }
  }
  if (plan_->GetJoinType() != JoinType::LEFT && plan_->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
  }
  return false;
}

}  // namespace bustub
