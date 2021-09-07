//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID tmp;
  finished_ = !left_executor_->Next(&left_tuple_, &tmp);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  RID tmp;
  while (!finished_) {
    if (!right_executor_->Next(&right_tuple, &tmp)) {
      right_executor_->Init();
      finished_ = !left_executor_->Next(&left_tuple_, &tmp);
      continue;
    }
    if (plan_->Predicate()
            ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                           right_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      *tuple = JoinTuple(&left_tuple_, &right_tuple);
      return true;
    }
  }
  return false;
}

Tuple NestedLoopJoinExecutor::JoinTuple(Tuple *left_tuple, Tuple *right_tuple) {
  std::vector<Value> values;
  for (auto const &col : GetOutputSchema()->GetColumns()) {
    values.emplace_back(col.GetExpr()->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                                    right_executor_->GetOutputSchema()));
  }

  return Tuple{values, GetOutputSchema()};
}

}  // namespace bustub
