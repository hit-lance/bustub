//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::scoped_lock guard(latch_);
  frame_id_t frame_id;
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    ++pages_[frame_id].pin_count_;
    return &pages_[frame_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[frame_id].ResetMemory();
  page_table_[page_id] = frame_id;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::scoped_lock guard(latch_);
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) != page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  std::scoped_lock guard(latch_);
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      all_pinned = false;
      break;
    }
  }
  if (all_pinned) {
    return nullptr;
  }
  frame_id_t frame_id;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.  if
  // (!free_list_.empty()) {
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = disk_manager_->AllocatePage();
  pages_[frame_id].ResetMemory();
  page_table_[*page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = *page_id;
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::scoped_lock guard(latch_);
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.find(page_id) != page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  replacer_->Unpin(frame_id);
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.emplace_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock guard(latch_);
  for (auto &item : page_table_) {
    disk_manager_->WritePage(item.first, pages_[item.second].data_);
    pages_[item.second].is_dirty_ = false;
  }
}

}  // namespace bustub
