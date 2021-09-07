//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    Tuple new_tuple(GenerateUpdatedTuple(*tuple));
    if (!table_meta_data_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
      throw std::runtime_error("update tuple failed. ");
    }
    // index
    for (auto &index_info : indexes_) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}
}  // namespace bustub
