//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock guard(latch_);
  if (list_.empty()) {
    return false;
  }
  *frame_id = list_.front();
  hash_table_.erase(list_.front());
  list_.pop_front();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock guard(latch_);
  if (hash_table_.find(frame_id) != hash_table_.end()) {
    list_.erase(hash_table_[frame_id]);
    hash_table_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock guard(latch_);
  if (hash_table_.find(frame_id) == hash_table_.end()) {
    list_.emplace_back(frame_id);
    hash_table_[frame_id] = std::prev(list_.end());
    assert(list_.size() <= num_pages_);
  }
}

size_t LRUReplacer::Size() { return list_.size(); }

}  // namespace bustub
