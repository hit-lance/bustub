//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"

#include <iostream>
#include <sstream>

#include "common/exception.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  array[0] = {KeyType{}, INVALID_PAGE_ID};
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { return array[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  return array[LookupIndex(key, comparator)].second;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookupIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    // if (array[mid].first > key)
    if (comparator(array[mid].first, key) == 1) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return left - 1;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0] = {KeyType{}, old_value};
  array[1] = {new_key, new_value};
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int insert_index = ValueIndex(old_value) + 1;
  for (int i = GetSize() - 1; i >= insert_index; --i) {
    array[i + 1] = array[i];
  }
  array[insert_index] = {new_key, new_value};
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int middle_key_index = (GetMaxSize() + 1) / 2;
  recipient->CopyNFrom(&array[middle_key_index], GetMaxSize() - middle_key_index, buffer_pool_manager);
  SetSize(middle_key_index);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents
 * page now changes to me. So I need to 'adopt' them by changing their parent
 * page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; ++i) {
    array[i] = items[i];
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(items[i].second)->GetData());
    assert(page != nullptr);
    page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(items[i].second, true);
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array[0].first = middle_key;
  for (int i = 0; i < GetSize(); ++i) {
    recipient->CopyLastFrom(array[i], buffer_pool_manager);
  }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  ValueType first_value = array[0].second;
  recipient->CopyFirstFrom({middle_key, first_value}, buffer_pool_manager);
  for (int i = 0; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
 * to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize());
  array[GetSize()] = pair;

  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  assert(page != nullptr);
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s
 * array to position the middle_key at the right place. You also need to use
 * BufferPoolManager to persist changes to the parent page id for those pages
 * that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  ValueType last_value = array[GetSize() - 1].second;
  recipient->CopyFirstFrom({middle_key, last_value}, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be
 * updated. So I need to 'adopt' it by changing its parent page id, which needs
 * to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize());
  for (int i = GetSize(); i > 1; --i) {
    array[i] = array[i - 1];
  }
  // array[1].second = array[0].second;
  // array[1].first = pair.first;
  // array[0].second = pair.second;
  array[1] = pair;
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  assert(page != nullptr);
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsSafe(BPlusTreeOpType op_type) {
  bool is_safe = true;
  if (op_type == BPlusTreeOpType::INSERT) {
    is_safe = GetSize() < GetMaxSize();
  } else if (op_type == BPlusTreeOpType::REMOVE) {
    is_safe = IsRootPage() ? GetSize() > 2 : (GetSize() > GetMinSize() && GetSize() > 2);
  }
  return is_safe;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
