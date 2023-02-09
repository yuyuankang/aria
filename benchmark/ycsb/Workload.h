//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace aria {

namespace ycsb {

class Workload {
public:
  Workload(std::size_t coordinator_id, Random &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id), random(random),
        partitioner(partitioner) {}

  std::unique_ptr<CalvinTransaction>
  next_transaction(const Context &context, std::size_t partition_id, Storage &storage) {

    return std::make_unique<CalvinYCSBTransaction>(
        coordinator_id, partition_id, context, random, partitioner,
        storage);

  }

private:
  std::size_t coordinator_id;
  Random &random;
  Partitioner &partitioner;
};

} // namespace ycsb
} // namespace aria
