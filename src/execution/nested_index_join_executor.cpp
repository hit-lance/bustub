//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  inner_table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_meta_data_->name_);
  assert(index_);
  child_executor_->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple left_tuple;
  RID tmp;
  while (child_executor_->Next(&left_tuple, &tmp)) {
    Tuple index_key = GenerateKeyTuple(left_tuple);
    std::vector<RID> result;
    index_->index_->ScanKey(index_key, &result, GetExecutorContext()->GetTransaction());
    if (!result.empty()) {
      Tuple right_tuple;
      inner_table_meta_data_->table_->GetTuple(result[0], &right_tuple, GetExecutorContext()->GetTransaction());
      *tuple = JoinTuple(&left_tuple, &right_tuple);
      return true;
    }
  }
  return false;
}

Tuple NestIndexJoinExecutor::GenerateKeyTuple(const Tuple &tuple) {
  std::cout << tuple.ToString(plan_->GetChildPlan()->OutputSchema()) << std::endl;
  std::cout << plan_->GetChildPlan()->OutputSchema()->ToString() << std::endl;
  std::cout << plan_->OuterTableSchema()->ToString() << std::endl;
  std::cout << plan_->InnerTableSchema()->ToString() << std::endl;
  std::cout << index_->key_schema_.ToString() << std::endl;
  std::vector<Value> values;
  for (auto &col : plan_->GetChildPlan()->OutputSchema()->GetColumns()) {
    assert(col.GetExpr());
    values.emplace_back(col.GetExpr()->Evaluate(&tuple, plan_->GetChildPlan()->OutputSchema()));
  }
  return {values, index_->index_->GetKeySchema()};
}

Tuple NestIndexJoinExecutor::JoinTuple(Tuple *left_tuple, Tuple *right_tuple) {
  std::vector<Value> values;
  for (auto const &col : GetOutputSchema()->GetColumns()) {
    values.emplace_back(
        col.GetExpr()->EvaluateJoin(left_tuple, plan_->OuterTableSchema(), right_tuple, plan_->InnerTableSchema()));
  }
  return Tuple{values, GetOutputSchema()};
}

}  // namespace bustub
