//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_{plan->GetAggregates(), plan->GetAggregateTypes()},
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_->Next(&tmp_tuple, &tmp_rid)) {
    aht_.InsertCombine(MakeKey(&tmp_tuple), MakeVal(&tmp_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  for (; aht_iterator_ != aht_.End(); ++aht_iterator_) {
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()
            ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
            .GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.push_back(
            col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
      }
      *tuple = Tuple(values, GetOutputSchema());
      ++aht_iterator_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
