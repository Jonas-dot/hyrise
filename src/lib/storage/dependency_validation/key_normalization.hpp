#pragma once

#include <cstdint>
#include <string>

namespace hyrise::dv_tree {

// Fixed DuckDB-compatible ascending ordering policy. Every non-NULL composite
// column starts with VALID and NULL is encoded as the larger, single-byte NULL
// marker (ASC NULLS LAST). Floating-point encodings canonicalize signed zero,
// infinities, and every NaN exactly as DuckDB's Radix encoder does.
inline constexpr char NORMALIZED_VALID_MARKER = '\x01';
inline constexpr char NORMALIZED_NULL_MARKER = '\x02';

// Order-preserving scalar encodings. Lexicographically comparing the emitted
// bytes produces the same total order as comparing the source values under the
// fixed policy above.
void normalize_uint32(uint32_t value, std::string& out);
void normalize_int32(int32_t value, std::string& out);
void normalize_int64(int64_t value, std::string& out);
void normalize_float(float value, std::string& out);
void normalize_double(double value, std::string& out);
void normalize_string_delimited(const std::string& value, std::string& out);
void normalize_null(std::string& out);

}  // namespace hyrise::dv_tree
