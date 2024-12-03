#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  auto comparator = [this](const Tuple &lhs, const Tuple &rhs) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      Value lhs_value = expr->Evaluate(&lhs, child_executor_->GetOutputSchema());
      Value rhs_value = expr->Evaluate(&rhs, child_executor_->GetOutputSchema());
      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC ||
               order_by_type == OrderByType::INVALID;
      }
      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue) {
        return order_by_type == OrderByType::DESC;
      }
    }
    UNREACHABLE("doesn't support duplicate key");
  };
  if (plan_->GetN() > sorted_tuples_.size()) {
    topn_tuples_.resize(sorted_tuples_.size());
  } else {
    topn_tuples_.resize(plan_->GetN());
  }
  std::partial_sort_copy(sorted_tuples_.begin(), sorted_tuples_.end(), topn_tuples_.begin(), topn_tuples_.end(),
                         comparator);
  topn_tuples_iter_ = topn_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (topn_tuples_iter_ == topn_tuples_.end()) {
    return false;
  }
  *tuple = *topn_tuples_iter_;
  *rid = tuple->GetRid();
  ++topn_tuples_iter_;
  return true;
}

}  // namespace bustub
