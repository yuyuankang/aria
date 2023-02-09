//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"

#include "benchmark/ycsb/Workload.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include <unordered_set>

namespace aria {

template<class Context>
class InferType {
};



template<>
class InferType<aria::ycsb::Context> {
public:
  template<class Transaction>
  using WorkloadType = aria::ycsb::Workload<Transaction>;
};

class WorkerFactory {

public:
  template<class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {
        "TwoPL",
        "Calvin", "Bohm", "Aria", "AriaFB", "Pwv"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    using WorkloadType =
        typename InferType<Context>::template WorkloadType<aria::CalvinTransaction>;

    // create manager

    auto manager = std::make_shared<CalvinManager<WorkloadType>>(
        coordinator_id, context.worker_num, db, context, stop_flag);

    // create worker

    std::vector<CalvinExecutor<WorkloadType> *> all_executors;

    for (auto i = 0u; i < context.worker_num; i++) {

      auto w = std::make_shared<CalvinExecutor<WorkloadType>>(
          coordinator_id, i, db, context, manager->transactions,
          manager->storages, manager->lock_manager_status,
          manager->worker_status, manager->n_completed_workers,
          manager->n_started_workers);
      workers.push_back(w);
      manager->add_worker(w);
      all_executors.push_back(w.get());
    }
    // push manager to workers
    workers.push_back(manager);

    for (auto i = 0u; i < context.worker_num; i++) {
      static_cast<CalvinExecutor<WorkloadType> *>(workers[i].get())
          ->set_all_executors(all_executors);
    }


    return workers;
  }
};
} // namespace aria