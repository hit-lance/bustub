//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_info_(exec_ctx_->GetCatalog()->GetIndex(plan->GetIndexOid())) {}

void IndexScanExecutor::Init() {
  table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  index_ = static_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get());
  cur_index_iter_ = index_->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  for (; cur_index_iter_ != index_->GetEndIterator(); ++cur_index_iter_) {
    *rid = (*cur_index_iter_).second;
    if (!table_meta_data_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())) {
      throw std::runtime_error("Failed to get tuple");
    }
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(tuple, &table_meta_data_->schema_).GetAs<bool>()) {
      ++cur_index_iter_;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
