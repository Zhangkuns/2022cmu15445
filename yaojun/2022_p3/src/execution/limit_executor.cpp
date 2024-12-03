#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (count_ < plan_->GetLimit() && child_executor_->Next(tuple, rid)) {
    count_++;
    return true;
  }
  return false;
}

}  // namespace bustub
