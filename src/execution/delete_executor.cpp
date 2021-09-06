//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (!table_meta_data_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      throw std::runtime_error("delete tuple failed. ");
    }
    // index
    for (auto &index_info : indexes_) {
      index_info->index_->DeleteEntry(
          tuple->KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
