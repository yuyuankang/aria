//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"

namespace aria {
namespace ycsb {

template<std::size_t N>
struct YCSBQuery {
  int32_t Y_KEY[N];
  bool UPDATE[N];
};

template<std::size_t N>
class YCSBQueryProducer {

public:
  static YCSBQuery<N> make_query(const Context &context, Random &random)  {
    YCSBQuery<N> query;
    int readOnly = random.uniform_dist(1, 100);

    for (auto i = 0u; i < N; i++) {
      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;

        // a uniform key is generated in three cases
        // case 1: it is a uniform distribution
        // case 2: the skew pattern is read, but this is a key for update
        // case 3: the skew pattern is write, but this is a kew for read
        // TODO only zipf
        key = Zipf::globalZipf().value(random.next_double());
        query.Y_KEY[i] = key;
        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
    return query;
  }
};
} // namespace ycsb
} // namespace aria
