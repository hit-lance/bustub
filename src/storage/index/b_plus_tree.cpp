//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  latch_.RLock();
  if (IsEmpty()) {
    latch_.RUnlock();
    return false;
  }
  Page *page = FindLeafPage(key);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value{};
  bool exist = leaf->Lookup(key, &value, comparator_);
  if (exist) {
    result->emplace_back(value);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  latch_.WLock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    latch_.WUnlock();
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate a new page.");
  }
  UpdateRootPageId(1);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  assert(!IsEmpty());
  transaction->AddIntoPageSet(nullptr);
  Page *page = FindLeafPage(key, BPlusTreeOpType::INSERT, transaction);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType tmp{};
  bool exist = leaf->Lookup(key, &tmp, comparator_);
  if (!exist) {
    assert(leaf->GetSize() < leaf_max_size_);
    leaf->Insert(key, value, comparator_);
    if (leaf->GetSize() == leaf_max_size_) {
      LeafPage *new_leaf = Split(leaf);
      new_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf->GetPageId());
      InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf);
    }
  }
  UnlockAncestorPages(!exist, transaction);
  return !exist;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  int page_id = INVALID_PAGE_ID;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate a new page.");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->Init(page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate a new page.");
    }
    UpdateRootPageId(0);
    InternalPage *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  assert(parent_page != nullptr);
  // assert(parent_page->GetPinCount() == 2);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent->GetSize() < internal_max_size_) {
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  } else {
    KeyType middle_key = parent->KeyAt((parent->GetMaxSize() + 1) / 2);
    InternalPage *new_inner = Split(parent);
    assert(comparator_(key, middle_key));
    if (comparator_(key, middle_key) == 1) {
      new_inner->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(new_inner->GetPageId());
    } else {
      parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    InsertIntoParent(parent, middle_key, new_inner);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  latch_.WLock();
  if (!IsEmpty()) {
    transaction->AddIntoPageSet(nullptr);
    Page *page = FindLeafPage(key, BPlusTreeOpType::REMOVE, transaction);
    assert(page != nullptr);
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    int old_leaf_size = leaf->GetSize();
    int new_leaf_size = leaf->RemoveAndDeleteRecord(key, comparator_);
    assert(new_leaf_size < leaf->GetMaxSize());
    bool deleted = old_leaf_size != new_leaf_size;
    if (deleted) {  // delete, and then CoalesceOrRedistribute
      // std::cout<<"deleted"<<std::endl;
      CoalesceOrRedistribute(leaf, transaction);
    }
    UnlockAncestorPages(deleted, transaction);
    const auto deleted_page_set = transaction->GetDeletedPageSet();
    for (const auto &page_id : *deleted_page_set) {
      buffer_pool_manager_->DeletePage(page_id);
    }
    deleted_page_set->clear();
  } else {
    latch_.WUnlock();
  }
}  // namespace bustub

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    bool root_deleted = AdjustRoot(node);
    if (root_deleted) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return root_deleted;
  }
  // have enough items after deletion, leaf page should not be deleted.
  if (node->GetSize() >= node->GetMinSize() && ((!node->IsLeafPage() && node->GetSize() > 1) || node->IsLeafPage())) {
    return false;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  assert(parent_page != nullptr);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  assert(parent->GetSize() > 1);
  int index = parent->ValueIndex(node->GetPageId());
  assert(index >= 0);
  int sibling_index = index ? index - 1 : 1;
  Page *silbing_page = buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index));
  assert(silbing_page != nullptr);
  N *sibling = reinterpret_cast<N *>(silbing_page->GetData());
  // std::cout<<sibling->GetSize()<< " "<<node->GetSize()<<std::endl;
  silbing_page->WLatch();
  bool need_redistribute = node->IsLeafPage() ? sibling->GetSize() + node->GetSize() >= node->GetMaxSize()
                                              : sibling->GetSize() + node->GetSize() > node->GetMaxSize();
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  if (need_redistribute) {
    Redistribute(sibling, node, index);
    silbing_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(silbing_page->GetPageId(), true);
    return false;
  }
  assert(parent_page->GetPinCount() > 0);
  Coalesce(&sibling, &node, &parent, index, transaction);
  silbing_page->WUnlatch();
  // std::cout<<node->GetPageId()<<" "<<sibling->GetPageId()<<std::endl;
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  bool swapped = false;
  if (index == 0) {  // node->neighbor_node, swap
    N *tmp_node = *neighbor_node;
    *neighbor_node = *node;
    *node = tmp_node;
    ++index;
    swapped = true;
  }

  // neighbor_node->node
  (*node)->MoveAllTo(*neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  (*parent)->Remove(index);
  // std::cout << (*parent)->GetSize() << std::endl;
  if (swapped) {
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
    buffer_pool_manager_->DeletePage((*node)->GetPageId());
  } else {
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
    transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  }
  // std::cout<<(*parent)->GetSize()<<" "<<(*parent)->GetPageId()<<std::endl;
  return CoalesceOrRedistribute(*parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  InternalPage *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  assert(parent != nullptr);
  assert(parent->GetSize() > 1);
  // std::cout<<neighbor_node->GetPageId()<<" "<<node->GetPageId()<<" "<<index<<" "<<std::endl;
  if (index == 0) {  // node->neighbor_node
    KeyType new_middle_key = neighbor_node->KeyAt(1);
    // std::cout<<parent->KeyAt(1)<<std::endl;
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(1, new_middle_key);
  } else {  // neighbor_node->node
    KeyType new_middle_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, new_middle_key);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {  // case 1
    InternalPage *inner = static_cast<InternalPage *>(old_root_node);
    root_page_id_ = inner->RemoveAndReturnOnlyChild();
    BPlusTreePage *new_root =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    assert(new_root != nullptr);
    new_root->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {  // case 2
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key{};
  latch_.RLock();
  Page *page = FindLeafPage(key, true);
  page_id_t page_id = page->GetPageId();
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return {page_id, 0, buffer_pool_manager_};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  latch_.RLock();
  Page *page = FindLeafPage(key);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t page_id = leaf->GetPageId();
  int index = leaf->KeyIndex(key, comparator_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return {page_id, index, buffer_pool_manager_};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return {INVALID_PAGE_ID, 0, buffer_pool_manager_}; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  page_id_t page_id = root_page_id_;
  latch_.RUnlock();
  Page *page = nullptr;

  while (true) {
    page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    page->RLatch();
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      break;
    }
    InternalPage *inner = static_cast<InternalPage *>(node);
    if (leftMost) {
      page_id = inner->ValueAt(0);
    } else {
      page_id = inner->Lookup(key, comparator_);
    }
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, BPlusTreeOpType op_type, Transaction *transaction) {
  assert(transaction != nullptr);
  page_id_t page_id = root_page_id_;
  Page *page = nullptr;
  while (true) {
    page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->WLatch();
    if (node->IsLeafPage()) {
      LeafPage *leaf = static_cast<LeafPage *>(node);

      if (leaf->IsSafe(op_type)) {
        UnlockAncestorPages(false, transaction);
      }
      transaction->AddIntoPageSet(page);
      break;
    }
    InternalPage *inner = static_cast<InternalPage *>(node);
    if (inner->IsSafe(op_type)) {
      UnlockAncestorPages(false, transaction);
    }
    transaction->AddIntoPageSet(page);
    page_id = inner->Lookup(key, comparator_);
  }
  // std::cout<<"break"<<std::endl;
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAncestorPages(bool is_dirty, Transaction *transaction) {
  std::shared_ptr<std::deque<Page *>> locked_pages = transaction->GetPageSet();
  while (!locked_pages->empty()) {
    Page *lp = locked_pages->front();
    locked_pages->pop_front();
    if (lp == nullptr) {
      latch_.WUnlock();
    } else {
      lp->WUnlatch();
      buffer_pool_manager_->UnpinPage(lp->GetPageId(), is_dirty);
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
