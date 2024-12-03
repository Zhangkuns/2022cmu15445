#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple &lhs, const Tuple &rhs) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      Value lhs_value = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      Value rhs_value = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC;
      }
      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DESC;
      }
    }
    UNREACHABLE("doesn't support duplicate key");
  });
  sorted_tuples_iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *sorted_tuples_iter_;
  ++sorted_tuples_iter_;
  return true;
}

}  // namespace bustub
