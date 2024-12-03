#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

void SortExecutor::Init() {
  child_->Init();
  Tuple child_tuple{};
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  std::sort(child_tuples_.begin(), child_tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool default_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if (expr->Evaluate(&a, child_->GetOutputSchema())
              .CompareLessThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return default_order_by;
      }
      if (expr->Evaluate(&a, child_->GetOutputSchema())
              .CompareGreaterThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return !default_order_by;
      }
    }

    return true;
  });

  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;

  return true;
}

}  // namespace bustub
