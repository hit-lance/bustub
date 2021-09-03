//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_meta_data_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      cur_table_iter_(table_meta_data_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  for (; cur_table_iter_ != table_meta_data_->table_->End(); ++cur_table_iter_) {
    if (plan_->GetPredicate()->Evaluate(&(*cur_table_iter_), &table_meta_data_->schema_).GetAs<bool>()) {
      *tuple = *cur_table_iter_;
      *rid = cur_table_iter_->GetRid();
      ++cur_table_iter_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
