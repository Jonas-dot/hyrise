#pragma once

// Standalone stand-in for Hyrise's src/lib/types.hpp. The DV-Tree core is
// copied verbatim from src/lib/storage/dependency_validation/ and consumes
// exactly these symbols: hyrise::CommitID (a strong typedef over uint32_t with
// the same conversion behavior as Hyrise's STRONG_TYPEDEF macro) plus the
// UNSET/INITIAL/MAX commit-ID constants. Nothing else from Hyrise is needed.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>

namespace hyrise {

struct CommitID {
  using base_type = uint32_t;
  base_type t;

  constexpr explicit CommitID(const base_type& value) noexcept : t(value) {}

  CommitID() noexcept : t() {}

  CommitID& operator=(const base_type& other) noexcept {
    t = other;
    return *this;
  }

  constexpr operator const base_type&() const {
    return t;
  }

  operator base_type&() {
    return t;
  }

  constexpr bool operator==(const CommitID& other) const {
    return t == other.t;
  }

  constexpr bool operator!=(const CommitID& other) const {
    return t != other.t;
  }

  constexpr bool operator<(const CommitID& other) const {
    return t < other.t;
  }

  constexpr bool operator>(const CommitID& other) const {
    return other.t < t;
  }

  constexpr bool operator<=(const CommitID& other) const {
    return !(other.t < t);
  }

  constexpr bool operator>=(const CommitID& other) const {
    return !(t < other.t);
  }
};

inline std::ostream& operator<<(std::ostream& stream, const CommitID& value) {
  return stream << value.t;
}

constexpr CommitID UNSET_COMMIT_ID = CommitID{0};
constexpr CommitID INITIAL_COMMIT_ID = CommitID{1};
constexpr CommitID MAX_COMMIT_ID = CommitID{std::numeric_limits<CommitID::base_type>::max() - 1};

}  // namespace hyrise

namespace std {

template <>
struct hash<::hyrise::CommitID> {
  size_t operator()(const ::hyrise::CommitID& value) const {
    return hash<::hyrise::CommitID::base_type>{}(value.t);
  }
};

template <>
struct numeric_limits<::hyrise::CommitID> {
  static constexpr ::hyrise::CommitID min() {
    return ::hyrise::CommitID{numeric_limits<::hyrise::CommitID::base_type>::min()};
  }

  static constexpr ::hyrise::CommitID max() {
    return ::hyrise::CommitID{numeric_limits<::hyrise::CommitID::base_type>::max()};
  }

  static constexpr ::hyrise::CommitID lowest() {
    return ::hyrise::CommitID{numeric_limits<::hyrise::CommitID::base_type>::lowest()};
  }
};

}  // namespace std
