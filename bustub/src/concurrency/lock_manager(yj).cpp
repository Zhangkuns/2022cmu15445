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
  // 读未提交
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
  // 读已提交
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 可重复读
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  /*单纯获取一个map: table_oid ---> request*/
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();  // 先把队列锁了
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      // 这个请求的granted一定为true,因为如果事务之前的请求没有被通过，
      // 事务会被阻塞在lockmanager中，不可能再去获取一把锁
      if (request->lock_mode_ == lock_mode) {  // 事务id和锁模式相同 不能重复
        lock_request_queue->latch_.unlock();   // 直接返回，而且已经有这把锁了。
        return true;
      }

      if (lock_request_queue->upgrading_ !=
          INVALID_TXN_ID) {  // 当前资源只能有一个在锁升级，是这个表的请求队列，不是某个请求事务
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 历史请求request是意向锁，则当前lock_mode必须能满足升级条件
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&

          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 先把历史同事务请求request从队列中移除
      lock_request_queue->request_queue_.remove(request);
      // 是已授权的事务request，所以要从表锁集合中删除，第三个参数是false表示删除
      InsertOrDeleteTableLockSet(txn, request, false);
      // 创建一个新加锁请求
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

      // 找到第一个未加锁的位置，锁升级优先级当前最高
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      // 将请求插入到该为止
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      // 标记资源为正在锁升级
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      // 条件变量等待模型
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);  // 唤醒
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;                  // 终止，升级失败
          lock_request_queue->request_queue_.remove(upgrade_lock_request);  // 从队列中移除请求
          lock_request_queue->cv_.notify_all();                             // 通知其他线程
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;  // 拿到锁，升级成功
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);  // 加入表锁集合

      if (lock_mode != LockMode::EXCLUSIVE) {  // 只要不是排它锁，就可以通知其他线程
        lock_request_queue->cv_.notify_all();
      }
      return true;  // 返回成功
    }
  }
  // 如果lock table中没有有重复的txn_id 直接加到末尾 等待锁授权
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;  // 成功授权
  // 处理表的只关注表
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

// 解表锁
auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 加的锁表的-----lock
  table_lock_map_latch_.lock();
  // 如果表里没有，则直接aborted
  // 输入参数：事务，表ID
  // 锁表中-------> 没有这个表
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 获取行上读锁集合 哈希表
  // 获取行上写锁集合 哈希表
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  // 行锁s和x任意不为空 都要abort掉
  if (!(s_row_lock_set->find(oid) == s_row_lock_set->end() || s_row_lock_set->at(oid).empty()) ||
      !(x_row_lock_set->find(oid) == x_row_lock_set->end() || x_row_lock_set->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 走到这里，说明行上的s和x锁已经空了，拿到表id对应的队列
  auto lock_request_queue = table_lock_map_[oid];

  // 对队列加锁
  lock_request_queue->latch_.lock();
  // 释放哈希表的锁
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    // 遍历请求队列
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // 如果历史请求与要解锁的事务ID相等，且也被授权了，那么就是这次要解锁的事务ID对象
      lock_request_queue->request_queue_.remove(lock_request);
      // 先从请求队列中 移除这个事务请求

      // 请求队列通知其他阻塞的事务
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      // 队列解锁

      // 还需要处理一下事务的状态，虽然已经从请求队列中移除了，但是事务的状态还没修改
      // 可重复读：S和X
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          // 读提交：X
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          // 读未提交：X
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        // 如果事务状态不为COMMITTED和ABORTED，则修改为SHRINKING收缩
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
          // 把锁状态设置为shrinking
        }
      }
      // 从表集合中删除请求队列
      InsertOrDeleteTableLockSet(txn, lock_request, false);
      // 返回解锁成功
      return true;
    }
  }

  // 如果事务txd_id不在请求队列的授权事务中 则abort
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 行不支持所有意向锁
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  // 读未提交隔离级别 不允许S IS SIX 为什么不允许呢？
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    // x和ix只能在growing阶段
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 读提交 shrinking可以加S和IS
  // 为什么shrinking阶段还能加锁呢？TODO(yao)：为什么shrinking阶段还能加锁？？？？？

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 可重复读 shrinking阶段不能加任何锁
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

  // 为什么加共享锁不用care表锁呢？S和IS和SIX
  // 行锁只支持 S 和 X 锁。在加 S 锁时，需要保证对表有锁（任意锁都行）；
  // 在加 X 锁时，需要保证对表有 X/IX/SIX 锁。
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  bool is_upgrade = false;
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    // 已经行请求列表中如果已经存在已有事务ID
    if (request->txn_id_ == txn->GetTransactionId()) {  // 如果lock table中有重复的txn_id
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      // 锁升级：把一个持有旧锁的同事务，升级为更严格的锁类型
      if (!(request->lock_mode_ == LockMode::INTENTION_SHARED &&
            (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE ||
             lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          !(request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 如果是锁升级，则把原来的事务请求先删了，
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);
      // 创建一个锁升级请求
      is_upgrade = true;
      break;
    }
  }
  // 如果lock table中没有有重复的txn_id
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  if (is_upgrade) {
    std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
    for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
         lr_iter++) {
      if (!(*lr_iter)->granted_) {
        break;  // 找到第一个没授权的事务
      }
    }
    // 插入第一个没授权的事务之前，表示正在锁升级
    lock_request_queue->request_queue_.insert(lr_iter, lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
  } else {
    lock_request_queue->request_queue_.push_back(lock_request);
  }

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  if (is_upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  // 行锁 哈希表不存在 直接abort
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];

  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  // 遍历请求队列判断
  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      // 移除 通知 解锁 锁状态变更---> unlock note
      lock_request_queue->request_queue_.remove(lock_request);

      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      InsertOrDeleteRowLockSet(txn, lock_request, false);
      return true;
    }
  }
  // 如果不在行锁 哈希表 中 直接abort
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 判断是否有环，如果有，返回最新的事务ID
  // 对于事务集合中的每一个事务ID，进行深度搜索 txn_id是个出参 表示最年轻（编号最大）的事务ID
  for (auto const &start_txn_id : txn_set_) {  // 所有事务集合
    if (Dfs(start_txn_id)) {  // TODO(yao): 为什么要从active_set_活跃事务集合中找，而不是从txn_set_，两者有什么区别呢？
      // 活跃事务是不是等价于会发生死锁的事务集合
      // 起点会影响判环的结果吗？如果图不连通 会有影响 这里是每个起点都判断

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

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  waits_for_.erase(txn_id);
  // 这种循环遍历的方式是不是太低效了
  for (auto a_txn_id : txn_set_) {
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  // 判环检测代码比较多
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    /* TODO(yao)：为什么要睡眠一段时间呢？*/
    {  // TODO(students): detect deadlock
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      /*行和表都要加锁*/

      for (auto &pair : table_lock_map_) {
        /*注意：这个无序集合，是局部变量，每次处理一个数据项*/
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();  // 请求队列先加锁
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {                  // 如果这个事务被授权了
            granted_set.emplace(lock_request->txn_id_);  // 加入授权集合中
          } else {
            // 请求的事务id---> 当前事务id
            /*TODO(yao)：为什么要把当前的事务ID，和表的关系建立起来呢？*/
            map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
            for (auto txn_id : granted_set) {  // 如果是没授权的事务，那么遍历已经授权的事务，
              // lock_request---> 表id

              // 有向图，箭头指向依赖方？
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        // 解锁当前资源的请求队列
        pair.second->latch_.unlock();
      }

      /*对于行map*/
      for (auto &pair : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
            for (auto txn_id : granted_set) {
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        pair.second->latch_.unlock();
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {  // 只要有环
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);  // 终止这个事务
        DeleteNode(txn_id);                        // 删除节点

        if (map_txn_oid_.count(txn_id) > 0) {  // 如果表还有事务 则通知 该数据项上的所有其他线程
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        }

        if (map_txn_rid_.count(txn_id) > 0) {  // 如果行还有事务 则通知 该数据项上的所有其他线程
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
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

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  // 授予锁的条件
  // 1. 前面事务都加锁
  // 2. 前面事务都兼容
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      switch (lock_request->lock_mode_) {  // T2 want
        case LockMode::SHARED:
          if (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lr->lock_mode_ == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lr->lock_mode_ != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  // TODO(yao): 这个要我们自己实现吗？这个函数的目的和作用是什么？插入或者删除表锁集合！
  // 从哪儿删和从哪儿插？从事务本身的集合中。
  // 要根据锁模式做区分...
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (insert) {
        txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (insert) {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      }
      break;
  }
}

void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

}  // namespace bustub
