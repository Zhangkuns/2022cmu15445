/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"

#include <random>
#include <thread>  // NOLINT

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

void TableLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on every table and then unlocks */
  auto task = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, TableLockTest1) { TableLockTest1(); }  // NOLINT

/** Upgrading single transaction from S -> X */
void TableLockUpgradeTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  auto txn1 = txn_mgr.Begin();

  /** Take S lock */
  auto take_s_lock = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid);
  EXPECT_EQ(true, take_s_lock);
  CheckTableLockSizes(txn1, 1, 0, 0, 0, 0);

  /** Upgrade S to X */
  auto upgrade_s_lock = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
  EXPECT_EQ(true, upgrade_s_lock);
  CheckTableLockSizes(txn1, 0, 1, 0, 0, 0);

  /** Clean up */
  txn_mgr.Commit(txn1);
  CheckCommitted(txn1);
  CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);

  delete txn1;
}
TEST(LockManagerTest, TableLockUpgradeTest2) { TableLockUpgradeTest1(); }  // NOLINT

void RowLockTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  table_oid_t oid = 0;
  RID rid{0, 0};

  int num_txns = 4;
  std::vector<Transaction *> txns;
  for (int i = 0; i < num_txns; i++) {
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  /** Each transaction takes an S lock on the same table and row and then unlocks */
  auto task = [&](int txn_id) {
    bool res;

    res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    ASSERT_EQ(true, txns[txn_id]->IsTableSharedLocked(oid));
    CheckGrowing(txns[txn_id]);

    res = lock_mgr.LockRow(txns[txn_id], LockManager::LockMode::SHARED, oid, rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[txn_id]);
    /** Lock set should be updated */
    auto is_row_shared_locked = txns[txn_id]->IsRowSharedLocked(oid, rid);
    ASSERT_EQ(true, is_row_shared_locked);

    res = lock_mgr.UnlockRow(txns[txn_id], oid, rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);
    /** Lock set should be updated */
    is_row_shared_locked = txns[txn_id]->IsRowSharedLocked(oid, rid);
    ASSERT_EQ(false, is_row_shared_locked);

    res = lock_mgr.UnlockTable(txns[txn_id], oid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[txn_id]);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_txns);

  for (int i = 0; i < num_txns; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_txns; i++) {
    threads[i].join();
    delete txns[i];
  }
}
TEST(LockManagerTest, RowLockTest3) { RowLockTest1(); }  // NOLINT

void TwoPLTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  RID rid0{0, 0};
  RID rid1{0, 1};

  auto *txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
  EXPECT_TRUE(res);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  EXPECT_TRUE(res);

  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 0);

  res = lock_mgr.LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnRowLockSize(txn, oid, 1, 1);

  res = lock_mgr.UnlockRow(txn, oid, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnRowLockSize(txn, oid, 0, 1);

  try {
    lock_mgr.LockRow(txn, LockManager::LockMode::SHARED, oid, rid0);
  } catch (TransactionAbortException &e) {
    CheckAborted(txn);
    CheckTxnRowLockSize(txn, oid, 0, 1);
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnRowLockSize(txn, oid, 0, 0);
  CheckTableLockSizes(txn, 0, 0, 0, 0, 0);

  delete txn;
}

TEST(LockManagerTest, TwoPLTest4) { TwoPLTest1(); }  // NOLINT

TEST(LockManagerTest, CompatibilityTest5) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<table_oid_t> oids;
  std::vector<Transaction *> txns;

  /** 10 tables */
  int num_oids = 10;
  for (int i = 0; i < num_oids; i++) {
    table_oid_t oid{static_cast<uint32_t>(i)};
    oids.push_back(oid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  auto task_exclusive = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::EXCLUSIVE, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  /** Each transaction takes an S lock on every table and then unlocks */
  auto task_shared = [&](int txn_id) {
    bool res;
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.LockTable(txns[txn_id], LockManager::LockMode::SHARED, oid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const table_oid_t &oid : oids) {
      res = lock_mgr.UnlockTable(txns[txn_id], oid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);

    /** All locks should be dropped */
    CheckTableLockSizes(txns[txn_id], 0, 0, 0, 0, 0);
  };

  std::vector<std::thread> threads;
  threads.reserve(num_oids);

  for (int i = 0; i < num_oids; i++) {
    if (i % 3 == 0) {
      threads.emplace_back(std::thread{task_exclusive, i});
    } else {
      threads.emplace_back(std::thread{task_shared, i});
    }
  }

  for (int i = 0; i < num_oids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_oids; i++) {
    delete txns[i];
  }
}

TEST(LockManagerTest, AbortTest6) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_FALSE(res);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  std::thread t3([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(70));
  txn1->SetState(TransactionState::ABORTED);

  t1.join();
  t2.join();
  t3.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, UpgradeTest7) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
  lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
  lock_mgr.UnlockTable(txn0, oid);
  txn_mgr.Commit(txn0);
  txn_mgr.Begin(txn0);
  CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    res = lock_mgr.UnlockTable(txn1, oid);
    EXPECT_TRUE(res);
    CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(70));

    res = lock_mgr.UnlockTable(txn2, oid);
    EXPECT_TRUE(res);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, UpgradeTest8) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t1([&]() {
    // bool res;
    // std::this_thread::sleep_for(std::chrono::milliseconds(60));
    // res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED, oid);
    // EXPECT_TRUE(res);

    // res = lock_mgr.UnlockTable(txn1, oid);
    // EXPECT_TRUE(res);
    // CheckTableLockSizes(txn0, 0, 0, 0, 0, 0);
    // CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    // txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    // bool res;
    // res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED, oid);
    // EXPECT_TRUE(res);
    // std::this_thread::sleep_for(std::chrono::milliseconds(70));

    // res = lock_mgr.UnlockTable(txn2, oid);
    // EXPECT_TRUE(res);
    // txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, MixedTestNoTimeLimit9) {
  const int num = 10;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  bustub->ExecuteSql("\\dt", writer);
  auto schema = "CREATE TABLE test_1 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  std::string query = "INSERT INTO test_1 VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }
  bustub->ExecuteSql(query, writer);
  schema = "CREATE TABLE test_2 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  bustub->ExecuteSql(query, writer);

  auto txn1 = bustub->txn_manager_->Begin();
  auto txn2 = bustub->txn_manager_->Begin();

  fmt::print("------\n");

  query = "delete from test_1 where x = 100;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn1);

  bustub->txn_manager_->Commit(txn1);
  fmt::print("txn1 commit\n");

  bustub->txn_manager_->Commit(txn2);
  fmt::print("txn2 commit\n");

  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, MixedTest9) {
  TEST_TIMEOUT_BEGIN
  const int num = 10;
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  bustub->ExecuteSql("\\dt", writer);
  auto schema = "CREATE TABLE test_1 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  std::string query = "INSERT INTO test_1 VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }
  bustub->ExecuteSql(query, writer);
  schema = "CREATE TABLE test_2 (x int, y int);";
  bustub->ExecuteSql(schema, writer);
  bustub->ExecuteSql(query, writer);

  auto txn1 = bustub->txn_manager_->Begin();
  auto txn2 = bustub->txn_manager_->Begin();

  fmt::print("------\n");

  query = "delete from test_1 where x = 100;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn2);

  query = "select * from test_1;";
  bustub->ExecuteSqlTxn(query, writer, txn1);

  bustub->txn_manager_->Commit(txn1);
  fmt::print("txn1 commit\n");

  bustub->txn_manager_->Commit(txn2);
  fmt::print("txn2 commit\n");

  delete txn1;
  delete txn2;
  TEST_TIMEOUT_FAIL_END(10000)
}

TEST(LockManagerTest, Isolation_RepeatableRead10) {
  const int num = 5;

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer1 = bustub::SimpleStreamWriter(result, true, " ");

  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer1);
  fmt::print("{}", result.str());

  std::cerr << "x: initialize data" << std::endl;
  std::string insert_query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < num; i++) {
    insert_query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      insert_query += ", ";
    } else {
      insert_query += ";";
    }
  }

  // Insert initial data
  {
    std::stringstream ss;
    auto writer2 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(insert_query, writer2, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  // Verify initial data
  {
    std::string select_query = "SELECT * FROM nft;";
    std::stringstream ss;
    auto writer3 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(select_query, writer3, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    fmt::print("--- YOUR RESULT ---\n{}\n", ss.str());
  }

  std::thread t0([&]() {
    std::string thread0_query = "select * from nft where id = 0";
    std::stringstream ss;
    auto writer4 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    fmt::print("txn thread t0 {}\n", thread0_query);
    bustub->ExecuteSqlTxn(thread0_query, writer4, txn);
    fmt::print("thread t0 result\n{}\n", ss.str());
    std::string s1 = ss.str();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ss.str("");

    fmt::print("txn thread t0 {}\n", thread0_query);
    bustub->ExecuteSqlTxn(thread0_query, writer4, txn);
    fmt::print("txn thread t0 result\n{}\n", ss.str());
    std::string s2 = ss.str();

    EXPECT_EQ(s1, s2);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t0 commit\n");
    delete txn;
  });

  std::thread t1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string thread1_query = "update nft set terrier = 1 where id = 0";
    std::stringstream ss;
    auto writer5 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

    fmt::print("txn thread t1 {}\n", thread1_query);
    bustub->ExecuteSqlTxn(thread1_query, writer5, txn);
    fmt::print("txn thread t1 result\n{}\n", ss.str());

    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t1 commit\n");
    delete txn;
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, Isolation_Readcommited11) {
  const int num = 5;

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer1 = bustub::SimpleStreamWriter(result, true, " ");

  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer1);
  fmt::print("{}", result.str());

  std::cerr << "x: initialize data" << std::endl;
  std::string insert_query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < num; i++) {
    insert_query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      insert_query += ", ";
    } else {
      insert_query += ";";
    }
  }

  {
    std::stringstream ss;
    auto writer2 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(insert_query, writer2, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  {
    std::string select_query = "SELECT * FROM nft;";
    std::stringstream ss;
    auto writer3 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(select_query, writer3, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    fmt::print("--- YOUR RESULT ---\n{}\n", ss.str());
  }

  std::thread t0([&]() {
    std::string thread0_query = "select * from nft";
    std::stringstream ss;
    auto writer4 = bustub::SimpleStreamWriter(ss, true);

    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::READ_COMMITTED);
    fmt::print("thread t0 {}\n", thread0_query);
    bustub->ExecuteSqlTxn(thread0_query, writer4, txn);
    fmt::print("thread t0 result 0 \n{}\n", ss.str());
    std::string s1 = ss.str();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ss.str("");  // Clear stream content
    fmt::print("txn {}\n", thread0_query);
    bustub->ExecuteSqlTxn(thread0_query, writer4, txn);
    fmt::print("thread t0 result 1 \n{}\n", ss.str());
    std::string s2 = ss.str();

    EXPECT_NE(s1, s2);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t0 commit\n");
    delete txn;
  });

  std::thread t1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string thread1_query = "update nft set terrier = 1 where id = 0";
    std::stringstream ss;
    auto writer5 = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

    fmt::print("txn thread t1 {}\n", thread1_query);
    bustub->ExecuteSqlTxn(thread1_query, writer5, txn);
    fmt::print("txn thread t1 result\n{}", ss.str());

    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t1 commit\n");
    delete txn;
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, CompatibilityTest12) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [S] SIX IS
  // [SIX IS]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CheckTableLockSizes(txn2, 0, 0, 1, 0, 0);
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, CompatibilityTest13) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [IS IX] SIX
  // [IS SIX]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CheckTableLockSizes(txn0, 0, 0, 1, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 1, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CheckTableLockSizes(txn0, 0, 0, 1, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 1);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, CompatibilityTest14) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [SIX] SIX IS
  // [SIX] [IS]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(res);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 1);
    CheckTableLockSizes(txn2, 0, 0, 1, 0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}
}  // namespace bustub
