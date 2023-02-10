//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinPartitioner.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include <thread>
#include <vector>

namespace aria {

class CalvinManager : public aria::Manager {
public:

  using TransactionType = CalvinTransaction;
//  static_assert(std::is_same<typename WorkloadType::TransactionType,
//                             TransactionType>::value,
//                "Transaction types do not match.");

  CalvinManager(std::size_t coordinator_id, std::size_t id, const aria::ycsb::Context &context,
                std::atomic<bool> &stopFlag)
      : aria::Manager(coordinator_id, id, context, stopFlag),
        partitioner(coordinator_id, context.coordinator_num,
                    CalvinHelper::string_to_vint(context.replica_group)) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
  }

  void coordinator_start() override {
    while (!stopFlag.load()) {

      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Analysis);
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction
      wait_all_workers_start();
      wait_all_workers_finish();

      // wait for all machines until they finish the analysis phase.
      wait4_ack();

      // Allow each worker to run transactions
      // DB is partitioned by the number of lock managers.
      // The first k workers act as lock managers to grant locks to other
      // workers The remaining workers run transactions upon assignment via the
      // queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      signal_worker(ExecutorStatus::Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
      // wait for all machines until they finish the execution phase.
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void add_worker(const std::shared_ptr<CalvinExecutor>

                  &w) {
    workers.push_back(w);
  }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

public:
  CalvinPartitioner partitioner;
  std::atomic<uint32_t> lock_manager_status;
  std::vector<std::shared_ptr<CalvinExecutor>> workers;
  std::vector<aria::ycsb::Storage> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace aria