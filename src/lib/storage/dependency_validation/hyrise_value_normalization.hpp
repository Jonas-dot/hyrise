#pragma once

#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise::dv_tree {

// Adapter boundary between Hyrise's runtime-typed AllTypeVariant/DataType and
// the fixed byte-level normalization policy implemented in
// key_normalization.hpp. The core primitives (normalize_int32, normalize_float,
// normalize_string_delimited, ...) are reused completely unchanged; this file
// only knows how to reach into an AllTypeVariant and pick the right one, so
// the byte-level policy itself cannot drift between the standalone core and
// Hyrise.
//
// Coverage decision: every AllTypeVariant alternative Hyrise defines today
// (Null, Int == int32_t, Long == int64_t, Float, Double, String == pmr_string)
// is normalized here. There is currently no unsupported-type case to reject at
// dependency registration: the switch in the .cpp file has no default arm, so
// adding a new DataType fails the build here instead of silently falling
// through to an unnormalized/misnormalized key.

// Appends one column's normalized bytes to `out`, including the leading
// valid/NULL marker. `column_type` is the dependency column's declared type.
// A NULL AllTypeVariant is normalized regardless of `column_type`; a non-NULL
// AllTypeVariant must actually hold `column_type`'s value type.
void normalize_all_type_variant(const AllTypeVariant& value, DataType column_type, std::string& out);

// Normalizes a whole LHS or RHS tuple (one or more columns, in column order)
// into one self-delimiting composite key byte string, using the same
// per-column framing as normalized_key.hpp's make_normalized_key(). Works for
// a single scalar column too (a one-element vector).
std::string normalize_dependency_key(const std::vector<AllTypeVariant>& values,
                                      const std::vector<DataType>& column_types);

}  // namespace hyrise::dv_tree
