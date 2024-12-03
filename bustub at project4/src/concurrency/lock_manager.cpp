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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /** 第一步，检查 txn 的状态。*/
  /** 若 txn 处于 Abort/Commit 状态，抛逻辑异常，不应该有这种情况出现。*/
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw Exception("Transaction is already aborted or committed");
    // return false;
  }
  /** 若 txn 处于 Shrinking 状态，则需要检查 txn 的隔离级别和当前锁请求类型*/
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  /** 第一步保证了锁请求、事务状态、事务隔离级别的兼容。正常通过第一步后，可以开始尝试获取锁*/
  /**************************************************************************************/
  /** 第二步，获取 table 对应的 lock request queue。*/
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  table_lock_map_latch_.unlock();
  /** 从 table_lock_map_ 中获取 table 对应的 lock request queue。
   * 注意需要对 map 加锁，并且为了提高并发性，在获取到 queue 之后立即释放 map 的锁。
   * 若 queue 不存在则创建。*/
  /**************************************************************************************/
  /** 第三步，检查当前 txn 是否已经持有 table 的锁。检查此锁请求是否为一次锁升级。*/
  /** 首先，记得对 queue 加锁。*/
  lock_request_queue->latch_.lock();
  /** granted 和 waiting 的锁请求均放在同一个队列里，我们需要遍历队列查看有没有与当前事务 id（我习惯叫做
   * tid）相同的请求。*/
  for (auto request = lock_request_queue->request_queue_.begin(); request != lock_request_queue->request_queue_.end();
       request++) {
    auto lock_request = *request;
    /** 首先，判断此前授予锁类型是否与当前请求锁类型相同。
     * 若相同，则代表是一次重复的请求，直接返回。否则进行下一步检查。*/
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      /** 接下来，判断当前资源上是否有另一个事务正在尝试升级（queue->upgrading_ == INVALID_TXN_ID）。
       * 若有，则终止当前事务，抛出 UPGRADE_CONFLICT 异常。因为不允许多个事务在同一资源上同时尝试锁升级。*/
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      /** 我们终于可以进行锁升级了。引用 Discord 里 15-445 TA 的原话，锁升级可以被拆分成三个步骤：
       * 1. 检查升级是否合法。
       * 2. 释放当前持有的锁。并在 queue 中标记我正在尝试升级。
       * 3. 等待新锁被授予。*/
      /** 判断升级锁的类型和之前锁是否兼容，不能反向升级。*/
      if ((lock_request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      /** 释放当前持有的锁，并在 queue 中标记我正在尝试升级。*/
      /** 删除旧的记录*/
      lock_request_queue->request_queue_.erase(request);
      /** 是已授权的事务request，所以要从表锁集合中删除*/
      InsertOrDeleteTableLockSet(txn, lock_request, EditType::DELETE);
      /**************************************************************************************/
      /**第四步，将锁请求加入请求队列。*/
      /** 添加新的记录*/
      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      // 找到第一个未加锁的位置，锁升级优先级当前最高
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      /** 将一条新的记录插入在队列最前面一条未授予的记录之前*/
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      /**************************************************************************************/
      /** 第五步，尝试获取锁。*/
      /** use parameter std::adopt_lock because the mutex being passed to the std::unique_lock constructor
       * is already locked by the current thread*/
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;             // 终止，升级失败
          lock_request_queue->request_queue_.remove(upgrade_request);  // 从队列中移除请求
          lock_request_queue->cv_.notify_all();                        // 通知其他线程
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;  // 升级成功
      upgrade_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_request, EditType::INSERT);
      if (lock_mode != LockMode::EXCLUSIVE) {  // 只要不是排它锁，就可以通知其他线程
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  /** 若队列中没有与当前事务 id 相同的请求，则代表是一次新的请求。*/
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  /** 将新的请求加入队列。*/
  lock_request_queue->request_queue_.push_back(new_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(new_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_request);  // 从队列中移除请求
      lock_request_queue->cv_.notify_all();                    // 通知其他线程
      return false;
    }
  }
  new_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, new_request, EditType::INSERT);
  if (lock_mode != LockMode::EXCLUSIVE) {  // 只要不是排它锁，就可以通知其他线程
    lock_request_queue->cv_.notify_all();
  }
  return true;
}
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  /** 检查是否持有要解锁的锁*/
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  /** 由于是 table lock，在释放时需要先检查其下的所有 row lock 是否已经释放。*/
  auto s_row_lock_set = txn->GetSharedRowLockSet()->find(oid);
  if (!(s_row_lock_set == txn->GetSharedRowLockSet()->end() || s_row_lock_set->second.empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto x_row_lock_set = txn->GetExclusiveRowLockSet()->find(oid);
  if (!(x_row_lock_set == txn->GetExclusiveRowLockSet()->end() || x_row_lock_set->second.empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  /** 找到对应请求队列，加锁request_queue，*/
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto request = lock_request_queue->request_queue_.begin(); request != lock_request_queue->request_queue_.end();
       request++) {
    auto lock_request = *request;
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      /** 删除加锁记录*/
      lock_request_queue->request_queue_.erase(request);
      /** 在条件变量上 notify_all() 唤醒所有等待的线程*/
      lock_request_queue->cv_.notify_all();
      /** 解锁 request queue*/
      lock_request_queue->latch_.unlock();
      /**判断事务是否需要进入SHRINKING状态。*/
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      InsertOrDeleteTableLockSet(txn, lock_request, EditType::DELETE);
      return true;
    }
  }
  /**  如果事务txd_id不在请求队列的授权事务中则abort*/
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /** 第一步，检查 txn 的状态。*/
  /** 若 txn 处于 Abort/Commit 状态，抛逻辑异常，不应该有这种情况出现。*/
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    throw Exception("Transaction is already aborted or committed");
    // return false;
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  /** 若 txn 处于 Shrinking 状态，则需要检查 txn 的隔离级别和当前锁请求类型*/
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 如果是排它锁
  if (lock_mode == LockMode::EXCLUSIVE) {  // 表 X IX SIX
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  // 如果是共享锁
  if (lock_mode == LockMode::SHARED) {  // 表 X IX SIX S IS
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) &&
        !txn->IsTableIntentionSharedLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  /** 第一步保证了锁请求、事务状态、事务隔离级别的兼容。正常通过第一步后，可以开始尝试获取锁*/
  /**************************************************************************************/
  /** 第二步，获取 row 对应的 lock request queue。*/
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  row_lock_map_latch_.unlock();
  /** 从 row_lock_map_ 中获取 row 对应的 lock request queue。
   * 注意需要对 map 加锁，并且为了提高并发性，在获取到 queue 之后立即释放 map 的锁。
   * 若 queue 不存在则创建。*/
  /**************************************************************************************/
  /** 第三步，检查当前 txn 是否已经持有 row 的锁。检查此锁请求是否为一次锁升级。*/
  /** 首先，记得对 queue 加锁。*/
  lock_request_queue->latch_.lock();
  /** granted 和 waiting 的锁请求均放在同一个队列里，我们需要遍历队列查看有没有与当前事务 id（我习惯叫做
   * tid）相同的请求。*/
  for (auto request = lock_request_queue->request_queue_.begin(); request != lock_request_queue->request_queue_.end();
       request++) {
    auto lock_request = *request;
    /** 首先，判断此前授予锁类型是否与当前请求锁类型相同。
     * 若相同，则代表是一次重复的请求，直接返回。否则进行下一步检查。*/
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      /** 接下来，判断当前资源上是否有另一个事务正在尝试升级（queue->upgrading_ == INVALID_TXN_ID）。
       * 若有，则终止当前事务，抛出 UPGRADE_CONFLICT 异常。因为不允许多个事务在同一资源上同时尝试锁升级。*/
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      /** 我们终于可以进行锁升级了。引用 Discord 里 15-445 TA 的原话，锁升级可以被拆分成三个步骤：
       * 1. 检查升级是否合法。
       * 2. 释放当前持有的锁。并在 queue 中标记我正在尝试升级。
       * 3. 等待新锁被授予。*/
      /** 判断升级锁的类型和之前锁是否兼容，不能反向升级。*/
      if ((lock_request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          (lock_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      /** 释放当前持有的锁，并在 queue 中标记我正在尝试升级。*/
      /** 删除旧的记录*/
      lock_request_queue->request_queue_.erase(request);
      /** 是已授权的事务request，所以要从表锁集合中删除*/
      InsertOrDeleteRowLockSet(txn, lock_request, EditType::DELETE);
      /**************************************************************************************/
      /**第四步，将锁请求加入请求队列。*/
      /** 添加新的记录*/
      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      // 找到第一个未加锁的位置，锁升级优先级当前最高
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      /** 将一条新的记录插入在队列最前面一条未授予的记录之前*/
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      /**************************************************************************************/
      /** 第五步，尝试获取锁。*/
      /** use parameter std::adopt_lock because the mutex being passed to the std::unique_lock constructor
       * is already locked by the current thread*/
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;             // 终止，升级失败
          lock_request_queue->request_queue_.remove(upgrade_request);  // 从队列中移除请求
          lock_request_queue->cv_.notify_all();                        // 通知其他线程
          return false;
        }
      }
      lock_request_queue->upgrading_ = INVALID_TXN_ID;  // 升级成功
      upgrade_request->granted_ = true;
      InsertOrDeleteRowLockSet(txn, upgrade_request, EditType::INSERT);
      if (lock_mode != LockMode::EXCLUSIVE) {  // 只要不是排它锁，就可以通知其他线程
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }
  /** 若队列中没有与当前事务 id 相同的请求，则代表是一次新的请求。*/
  auto new_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  /** 将新的请求加入队列。*/
  lock_request_queue->request_queue_.push_back(new_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(new_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_request);  // 从队列中移除请求
      lock_request_queue->cv_.notify_all();                    // 通知其他线程
      return false;
    }
  }
  new_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, new_request, EditType::INSERT);
  if (lock_mode != LockMode::EXCLUSIVE) {  // 只要不是排它锁，就可以通知其他线程
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  /** 检查是否持有要解锁的锁*/
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  /** 找到对应请求队列，加锁request_queue，*/
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  for (auto request = lock_request_queue->request_queue_.begin(); request != lock_request_queue->request_queue_.end();
       request++) {
    auto lock_request = *request;
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      /** 删除加锁记录*/
      lock_request_queue->request_queue_.erase(request);
      /** 在条件变量上 notify_all() 唤醒所有等待的线程*/
      lock_request_queue->cv_.notify_all();
      /** 解锁 request queue*/
      lock_request_queue->latch_.unlock();
      /**判断事务是否需要进入SHRINKING状态。*/
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
            txn->SetState(TransactionState::SHRINKING);  // 把锁状态设置为shrinking
          }
        }
      }
      InsertOrDeleteRowLockSet(txn, lock_request, EditType::DELETE);
      return true;
    }
  }
  /**  如果事务txd_id不在请求队列的授权事务中则abort*/
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  for (auto &txn : waits_for_[t1]) {
    if (txn == t2) {
      return;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (*it == t2) {
      waits_for_[t1].erase(it);
      return;
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::unordered_set<txn_id_t> visited;
  std::unordered_set<txn_id_t> rec_stack;
  visited.clear();
  rec_stack.clear();
  for (auto &start_txn : txn_set_) {
    /** 将每个事务作为起点*/
    if (visited.find(start_txn) == visited.end() && DFS(start_txn, visited, rec_stack)) {
      *txn_id = *active_set_.begin();  // 如果有环 则从活跃的事务集合里，找到最大的事务ID，最年轻，就是事务号最大
      for (auto const &active_txn_id : active_set_) {
        *txn_id = std::max(*txn_id, active_txn_id);
      }
      active_set_.clear();
      return true;
    }
    active_set_.clear();
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  if (waits_for_.empty()) {
    std::cout << "No edge in the graph" << std::endl;
    return result;
  }
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}

auto LockManager::DFS(txn_id_t txn_id, std::unordered_set<txn_id_t> &visited, std::unordered_set<txn_id_t> &rec_stack)
    -> bool {
  if (safe_set_.find(txn_id) != safe_set_.end()) {
    return false;
  }
  if (visited.find(txn_id) == visited.end()) {
    /** Mark the current node as visited and part of recursion stack*/
    visited.insert(txn_id);
    rec_stack.insert(txn_id);
    std::vector<txn_id_t> &next_node_vector = waits_for_[txn_id];
    std::sort(next_node_vector.begin(), next_node_vector.end());
    auto itr = next_node_vector.begin();
    for (; itr != next_node_vector.end(); itr++) {
      if (visited.find(*itr) == visited.end()) {
        if (DFS(*itr, visited, rec_stack)) {
          active_set_.insert(txn_id);
          return true;
        }
      }
      if (rec_stack.find(*itr) != rec_stack.end()) {
        active_set_.insert(txn_id);
        return true;
      }
    }
  }
  rec_stack.erase(txn_id);
  return false;
}
auto LockManager::DeleteNode(txn_id_t t1) -> void {
  for (auto a_txn_id : txn_set_) {
    if (a_txn_id != t1) {
      RemoveEdge(a_txn_id, t1);
    }
  }
  waits_for_.erase(t1);
  txn_set_.erase(t1);
}

void LockManager::StartCycleDetection() {
  if (!enable_cycle_detection_) {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }
}

void LockManager::StopCycleDetection() {
  if (enable_cycle_detection_) {
    enable_cycle_detection_ = false;
    if (cycle_detection_thread_ != nullptr && cycle_detection_thread_->joinable()) {
      cycle_detection_thread_->join();
    }
    delete cycle_detection_thread_;
    cycle_detection_thread_ = nullptr;
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      /** 遍历所有request_queue，建立一个 wait-for graph 。*/
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (const auto &table_lock_request : table_lock_map_) {
        table_lock_request.second->latch_.lock();
        std::unordered_set<txn_id_t> granted_set;
        for (const auto &request : table_lock_request.second->request_queue_) {
          if (request->granted_) {
            granted_set.insert(request->txn_id_);
          }
        }
        for (const auto &request : table_lock_request.second->request_queue_) {
          if (!request->granted_) {
            map_txn_oid_.emplace(request->txn_id_, request->oid_);
            for (auto granted_txn_id : granted_set) {
              AddEdge(request->txn_id_, granted_txn_id);
            }
          }
        }
        table_lock_request.second->latch_.unlock();
      }
      for (const auto &row_lock_request : row_lock_map_) {
        row_lock_request.second->latch_.lock();
        std::unordered_set<txn_id_t> granted_set;
        for (const auto &request : row_lock_request.second->request_queue_) {
          if (request->granted_) {
            granted_set.insert(request->txn_id_);
          }
        }
        for (const auto &request : row_lock_request.second->request_queue_) {
          if (!request->granted_) {
            map_txn_rid_.emplace(request->txn_id_, request->rid_);
            for (const auto &granted_txn_id : granted_set) {
              AddEdge(request->txn_id_, granted_txn_id);
            }
          }
        }
        row_lock_request.second->latch_.unlock();
      }
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
      /** 检查是否有环*/
      txn_id_t aborted_txn_id;
      while (HasCycle(&aborted_txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(aborted_txn_id);
        txn->SetState(TransactionState::ABORTED);  // 终止这个事务
        DeleteNode(aborted_txn_id);
        if (map_txn_oid_.count(aborted_txn_id) != 0) {
          table_lock_map_[map_txn_oid_[aborted_txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[aborted_txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[aborted_txn_id]]->latch_.unlock();
        }
        if (map_txn_rid_.count(aborted_txn_id) != 0) {
          row_lock_map_[map_txn_rid_[aborted_txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[aborted_txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[aborted_txn_id]]->latch_.unlock();
        }
      }
      // 清空数据
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

auto LockManager::InsertOrDeleteTableLockSet(Transaction *txn, std::shared_ptr<LockRequest> &lock_request,
                                             LockManager::EditType edit) -> void {
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (edit == EditType::INSERT) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (edit == EditType::INSERT) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (edit == EditType::INSERT) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (edit == EditType::INSERT) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (edit == EditType::INSERT) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

auto LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           EditType edit) -> void {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (edit == EditType::INSERT) {
        auto row_lock_set = s_row_lock_set->find(lock_request->oid_);
        if (row_lock_set == s_row_lock_set->end()) {
          s_row_lock_set->emplace(lock_request->oid_, std::unordered_set<RID>{});
          row_lock_set = s_row_lock_set->find(lock_request->oid_);
        }
        row_lock_set->second.emplace(lock_request->rid_);
      } else {
        auto row_lock_set = s_row_lock_set->find(lock_request->oid_);
        if (row_lock_set == s_row_lock_set->end()) {
          break;
        }
        row_lock_set->second.erase(lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (edit == EditType::INSERT) {
        auto row_lock_set = x_row_lock_set->find(lock_request->oid_);
        if (row_lock_set == x_row_lock_set->end()) {
          x_row_lock_set->emplace(lock_request->oid_, std::unordered_set<RID>{});
          row_lock_set = x_row_lock_set->find(lock_request->oid_);
        }
        row_lock_set->second.emplace(lock_request->rid_);
      } else {
        auto row_lock_set = x_row_lock_set->find(lock_request->oid_);
        if (row_lock_set == x_row_lock_set->end()) {
          break;
        }
        row_lock_set->second.erase(lock_request->rid_);
      }
      break;

    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  /** 判断优先级。锁请求会以严格的 FIFO 顺序依次满足。
   * 只有当前请求为请求队列中优先级最高的请求时，才允许授予锁。*/
  /** 如果队列中存在锁升级请求，若锁升级请求正为当前请求，则优先级最高。
   * 否则代表其他事务正在尝试锁升级，优先级高于当前请求。*/
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ != lock_request->txn_id_) {
      return false;
    }
  }
  /** 若队列中不存在锁升级请求，则遍历队列。
   * 如果，当前请求是第一个 waiting 状态的请求，则代表优先级最高。
   * 如果当前请求前面还存在其他 waiting 请求，则要判断当前请求是否前面的 waiting
   * 请求兼容。若兼容，则仍可以视为优先级最高。 若存在不兼容的请求，则优先级不为最高。*/
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      /** 判断兼容性。
       * 遍历请求队列，查看当前锁请求是否与所有的已经 granted 的请求兼容。
       * 需要注意的是，在我的实现中 granted 请求不一定都在队列头部，因此需要完全遍历整条队列。
       * 若全部兼容，则通过检查。否则直接返回 false。当前请求无法被满足。*/
      if (lr->lock_mode_ == LockMode::INTENTION_SHARED) {
        if (lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
        if (lock_request->lock_mode_ == LockMode::SHARED ||
            lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (lr->lock_mode_ == LockMode::SHARED) {
        if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
            lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || lock_request->lock_mode_ == LockMode::SHARED ||
            lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
      }
      if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
