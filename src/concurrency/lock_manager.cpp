//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  std::unique_lock<std::mutex> lk(latch_);

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  auto lrq = &lock_table_.find(rid)->second;
  lrq->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  auto iter = std::prev(lrq->request_queue_.end());

  // wait and grant
  lrq->cv_.wait(lk, [&] { return txn->GetState() == TransactionState::ABORTED || !lrq->exclusive_lock_granted_; });

  // check deadlock
  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->request_queue_.erase(iter);
    return false;
  }

  txn->GetSharedLockSet()->emplace(rid);
  ++lrq->shared_lock_cnt_;
  iter->granted_ = true;

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  std::unique_lock<std::mutex> lk(latch_);

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
  auto lrq = &lock_table_.find(rid)->second;
  lrq->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  auto iter = std::prev(lrq->request_queue_.end());

  // wait and grant
  lrq->cv_.wait(lk, [&] {
    return txn->GetState() == TransactionState::ABORTED ||
           (lrq->shared_lock_cnt_ == 0 && !lrq->exclusive_lock_granted_);
  });

  // check deadlock
  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->request_queue_.erase(iter);
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  lrq->exclusive_lock_granted_ = true;
  iter->granted_ = true;

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end()) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  std::unique_lock<std::mutex> lk(latch_);
  auto lrq = &lock_table_.find(rid)->second;

  if (lrq->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  // upgrade
  txn->GetSharedLockSet()->erase(rid);
  --lrq->shared_lock_cnt_;
  auto iter = find_if(lrq->request_queue_.begin(), lrq->request_queue_.end(),
                      [&](const LockRequest &lr) { return lr.txn_id_ == txn->GetTransactionId(); });
  iter->lock_mode_ = LockMode::EXCLUSIVE;
  iter->granted_ = false;
  lrq->upgrading_ = true;

  // wait and grant
  lrq->cv_.wait(lk, [&] {
    return txn->GetState() == TransactionState::ABORTED ||
           (lrq->shared_lock_cnt_ == 0 && !lrq->exclusive_lock_granted_);
  });

  // check deadlock
  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->request_queue_.erase(iter);
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  lrq->exclusive_lock_granted_ = true;
  iter->granted_ = true;
  lrq->upgrading_ = false;

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end() &&
      txn->GetExclusiveLockSet()->find(rid) == txn->GetExclusiveLockSet()->end()) {
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  std::unique_lock<std::mutex> lk(latch_);
  auto lrq = &lock_table_.find(rid)->second;

  auto iter = find_if(lrq->request_queue_.begin(), lrq->request_queue_.end(),
                      [&](const LockRequest &lr) { return lr.txn_id_ == txn->GetTransactionId(); });

  LockMode lock_mode = iter->lock_mode_;
  lrq->request_queue_.erase(iter);

  // Shared locks are released immediately when IsolationLevel==READ_COMMITTED.
  if (!(lock_mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (lock_mode == LockMode::SHARED) {
    if (--lrq->shared_lock_cnt_ == 0) {
      lrq->cv_.notify_all();
    }
  } else {
    lrq->exclusive_lock_granted_ = false;
    lrq->cv_.notify_all();
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

void LockManager::RemoveNode(txn_id_t t) {
  waits_for_.erase(t);
  for (auto &adj : waits_for_) {
    RemoveEdge(adj.first, t);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edge_list;
  for (auto &adj : waits_for_) {
    for (auto &node : adj.second) {
      edge_list.emplace_back(adj.first, node);
    }
  }
  return edge_list;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // construct wait-for graph
      for (auto &item : lock_table_) {
        std::vector<txn_id_t> granted_txn;
        auto lrq = &item.second.request_queue_;
        auto iter = lrq->begin();
        for (; iter != lrq->end(); ++iter) {
          if (!iter->granted_) {
            break;
          }
          granted_txn.emplace_back(iter->txn_id_);
        }
        for (; iter != lrq->end(); ++iter) {
          for (auto &gt : granted_txn) {
            AddEdge(iter->txn_id_, gt);
          }
        }
      }

      // check and break cycle
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        RemoveNode(txn_id);
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        for (auto &rid : *txn->GetSharedLockSet()) {
          lock_table_[rid].request_queue_.remove_if([&](LockRequest &lr) { return lr.txn_id_ == txn_id; });
          --lock_table_[rid].shared_lock_cnt_;
          lock_table_[rid].cv_.notify_all();
        }
        for (auto &rid : *txn->GetExclusiveLockSet()) {
          lock_table_[rid].request_queue_.remove_if([&](LockRequest &lr) { return lr.txn_id_ == txn_id; });
          lock_table_[rid].exclusive_lock_granted_ = false;
          lock_table_[rid].cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
