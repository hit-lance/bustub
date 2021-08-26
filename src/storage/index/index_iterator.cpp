/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id_ == INVALID_PAGE_ID && index_ == 0; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  LeafPage *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return leaf->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  assert(!isEnd());
  LeafPage *leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
  if (index_ < leaf->GetSize() - 1) {
    ++index_;
  } else {
    page_id_ = leaf->GetNextPageId();
    index_ = 0;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
