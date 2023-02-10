//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/ClassOf.h"
#include "common/Encoder.h"
#include "common/HashMap.h"
#include "common/MVCCHashMap.h"
#include "common/StringPiece.h"
#include <memory>

#include "core/Context.h"

namespace aria {

class ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~ITable() = default;

  virtual std::tuple<MetaDataType *, void *> search(const void *key,
                                                    uint64_t version = 0) = 0;

  virtual void *search_value(const void *key, uint64_t version = 0) = 0;

  virtual MetaDataType &search_metadata(const void *key,
                                        uint64_t version = 0) = 0;

  virtual std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                         uint64_t version) = 0;

  virtual void *search_value_prev(const void *key, uint64_t version) = 0;

  virtual MetaDataType &search_metadata_prev(const void *key,
                                             uint64_t version) = 0;

  virtual void insert(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void update(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void garbage_collect(const void *key) = 0;

  virtual void deserialize_value(const void *key, StringPiece stringPiece,
                                 uint64_t version = 0) = 0;

  virtual void serialize_value(Encoder &enc, const void *value) = 0;

  virtual std::size_t key_size() = 0;

  virtual std::size_t value_size() = 0;

  virtual std::size_t field_size() = 0;

  virtual std::size_t tableID() = 0;

  virtual std::size_t partitionID() = 0;
};

/* parameter version is not used in Table. */
template<std::size_t N>
class Table : public ITable {
public:

  virtual ~Table() override = default;

  Table(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    return std::get<0>(map_[k]);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    const auto &v = *static_cast<const ycsb::ycsb::value *>(value);
    DCHECK(map_.contains(k) == false);
    auto &row = map_[k];
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    const auto &v = *static_cast<const ycsb::ycsb::value *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {}

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {

    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const ycsb::ycsb::key *>(key);
    auto &row = map_[k];
    auto &v = std::get<1>(row);

    Decoder dec(stringPiece);
    dec >> v;

    DCHECK(size - dec.size() == ClassOf<ycsb::ycsb::value>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {

    std::size_t size = enc.size();
    const auto &v = *static_cast<const ycsb::ycsb::value *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ycsb::ycsb::value>::size());
  }

  std::size_t key_size() override { return sizeof(ycsb::ycsb::key); }

  std::size_t value_size() override { return sizeof(ycsb::ycsb::value); }

  std::size_t field_size() override { return ClassOf<ycsb::ycsb::value>::size(); }

  std::size_t tableID() override { return tableID_; }

  std::size_t partitionID() override { return partitionID_; }

private:
  HashMap<N, ycsb::ycsb::key, std::tuple<std::atomic<uint64_t>, ycsb::ycsb::value>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};

} // namespace aria
