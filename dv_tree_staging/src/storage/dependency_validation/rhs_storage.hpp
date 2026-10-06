#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace hyrise::dv_tree {

struct RhsRef {
  const uint8_t* data = nullptr;
  uint32_t size = 0;
};

inline int compare_rhs_bytes(const uint8_t* left, std::size_t left_size, const uint8_t* right, std::size_t right_size) {
  const std::size_t shared = std::min(left_size, right_size);
  const int prefix = shared == 0 ? 0 : std::memcmp(left, right, shared);
  if (prefix != 0)
    return prefix;
  if (left_size < right_size)
    return -1;
  if (left_size > right_size)
    return 1;
  return 0;
}

// Orders raw normalized byte strings by the same rule as RhsRefLess, for the
// containers that hold owned std::string keys rather than interned RhsRefs.
struct ByteStringLess {
  using is_transparent = void;

  bool operator()(std::string_view left, std::string_view right) const {
    return compare_rhs_bytes(reinterpret_cast<const uint8_t*>(left.data()), left.size(),
                             reinterpret_cast<const uint8_t*>(right.data()), right.size()) < 0;
  }
};

struct RhsRefLess {
  using is_transparent = void;

  bool operator()(RhsRef left, RhsRef right) const {
    return compare_rhs_bytes(left.data, left.size, right.data, right.size) < 0;
  }

  bool operator()(RhsRef left, std::string_view right) const {
    return compare_rhs_bytes(left.data, left.size, reinterpret_cast<const uint8_t*>(right.data()), right.size()) < 0;
  }

  bool operator()(std::string_view left, RhsRef right) const {
    return compare_rhs_bytes(reinterpret_cast<const uint8_t*>(left.data()), left.size(), right.data, right.size) < 0;
  }
};

// Each distinct RHS owns one allocation. The vector may move its unique_ptrs,
// but referenced byte allocations never move, so optimistic readers never need
// to resolve a ref through reallocating arena metadata.
class StableRhsStorage {
 public:
  struct MemoryStats {
    std::size_t allocations = 0;
    std::size_t payload_bytes = 0;
    std::size_t owner_array_bytes = 0;
  };

  RhsRef store(std::string_view bytes) {
    if (bytes.size() > std::numeric_limits<uint32_t>::max()) {
      throw std::length_error("normalized RHS exceeds RhsRef's 32-bit size");
    }
    auto allocation = std::make_unique<uint8_t[]>(bytes.empty() ? 1 : bytes.size());
    if (!bytes.empty())
      std::memcpy(allocation.get(), bytes.data(), bytes.size());
    RhsRef ref{allocation.get(), static_cast<uint32_t>(bytes.size())};
    allocations_.push_back(std::move(allocation));
    payload_bytes_ += bytes.empty() ? 1 : bytes.size();
    return ref;
  }

  // The caller owns synchronization for this append-only container.
  MemoryStats memory_stats() const {
    return {allocations_.size(), payload_bytes_, allocations_.capacity() * sizeof(decltype(allocations_)::value_type)};
  }

 private:
  std::vector<std::unique_ptr<uint8_t[]>> allocations_;
  std::size_t payload_bytes_ = 0;
};

}  // namespace hyrise::dv_tree
