#pragma once

#include <cstdint>
#include <limits>

// The isolated imported regression targets do not link the complete Hyrise
// dependency graph. This small test-only shape mirrors the operations of
// Hyrise's STRONG_TYPEDEF(uint32_t, CommitID). Production compilation always
// includes src/lib/types.hpp instead.
namespace hyrise {

struct CommitID {
  using base_type = uint32_t;

  constexpr explicit CommitID(const base_type value) : value_{value} {}

  constexpr CommitID() = default;

  constexpr operator const base_type&() const {
    return value_;
  }

  constexpr operator base_type&() {
    return value_;
  }

  constexpr bool operator==(const CommitID&) const = default;
  constexpr auto operator<=>(const CommitID&) const = default;

 private:
  base_type value_{0};
};

inline constexpr CommitID UNSET_COMMIT_ID{0};
inline constexpr CommitID INITIAL_COMMIT_ID{1};
inline constexpr CommitID MAX_COMMIT_ID{std::numeric_limits<CommitID::base_type>::max() - 1};

}  // namespace hyrise
