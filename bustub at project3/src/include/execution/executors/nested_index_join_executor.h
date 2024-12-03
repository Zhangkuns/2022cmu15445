//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  /** The left child executor that produces tuples for the join. */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The table information. */
  TableInfo *table_info_;
  /** The index information. */
  IndexInfo *index_info_;
  /** The left tuples produced by the left child executor. */
  std::vector<Tuple> left_tuples_;
  /** The right tuples produced by the right child executor. */
  std::vector<Tuple> right_tuples_;
  /** The index of the current left tuple. */
  size_t left_index_ = 0;
};
}  // namespace bustub
