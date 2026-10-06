#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "storage/dependency_validation/key_normalization.hpp"

namespace hyrise::dv_tree {

// Each composite column starts with a validity marker. Variable-length values
// are self-delimiting, so concatenation cannot confuse ("a", "bc") with
// ("ab", "c"). NULL is the single larger marker, implementing a fixed ASC
// NULLS LAST order. Byte equality therefore gives literal/IS NOT DISTINCT FROM
// semantics for NULL and canonical NaN values.
inline void append_normalized_column(std::string& out, int32_t value) {
  out.push_back(NORMALIZED_VALID_MARKER);
  normalize_int32(value, out);
}

inline void append_normalized_column(std::string& out, int64_t value) {
  out.push_back(NORMALIZED_VALID_MARKER);
  normalize_int64(value, out);
}

inline void append_normalized_column(std::string& out, float value) {
  out.push_back(NORMALIZED_VALID_MARKER);
  normalize_float(value, out);
}

inline void append_normalized_column(std::string& out, double value) {
  out.push_back(NORMALIZED_VALID_MARKER);
  normalize_double(value, out);
}

inline void append_normalized_column(std::string& out, const std::string& value) {
  out.push_back(NORMALIZED_VALID_MARKER);
  normalize_string_delimited(value, out);
}

inline void append_normalized_column(std::string& out, const char* value) {
  append_normalized_column(out, std::string(value));
}

template <typename T>
void append_normalized_column(std::string& out, const std::optional<T>& value) {
  if (value.has_value())
    append_normalized_column(out, *value);
  else
    normalize_null(out);
}

inline void append_normalized_columns(std::string&) {}

template <typename First, typename... Rest>
void append_normalized_columns(std::string& out, const First& first, const Rest&... rest) {
  append_normalized_column(out, first);
  append_normalized_columns(out, rest...);
}

template <typename... Columns>
std::string make_normalized_key(const Columns&... columns) {
  std::string result;
  append_normalized_columns(result, columns...);
  return result;
}

}  // namespace hyrise::dv_tree
