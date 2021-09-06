//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
  if (plan_->IsRawInsert()) {
    insert_values_ = plan_->RawValues();
    value_iter_ = insert_values_.begin();
  } else {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (value_iter_ != insert_values_.end()) {
      Tuple new_tuple(*value_iter_++, &(table_meta_data_->schema_));
      InsertTuple(new_tuple, rid);
      return true;
    }
    return false;
  }
  if (child_executor_->Next(tuple, rid)) {
    InsertTuple(*tuple, rid);
    return true;
  }
  return false;
}

void InsertExecutor::InsertTuple(Tuple &tuple, RID *rid) {
  if (!table_meta_data_->table_->InsertTuple(tuple, rid, exec_ctx_->GetTransaction())) {
    throw std::runtime_error("insert tuple failed. ");
  }
  // index
  for (auto &index_info : indexes_) {
    index_info->index_->InsertEntry(
        tuple.KeyFromTuple(table_meta_data_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
