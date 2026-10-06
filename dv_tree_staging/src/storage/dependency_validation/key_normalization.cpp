#include "storage/dependency_validation/key_normalization.hpp"

#include <cmath>
#include <cstring>
#include <limits>

namespace hyrise::dv_tree {

namespace {

void append_uint64_be(uint64_t value, std::string& out) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

}  // namespace

void normalize_uint32(uint32_t value, std::string& out) {
  out.push_back(static_cast<char>((value >> 24) & 0xFFu));
  out.push_back(static_cast<char>((value >> 16) & 0xFFu));
  out.push_back(static_cast<char>((value >> 8) & 0xFFu));
  out.push_back(static_cast<char>(value & 0xFFu));
}

void normalize_int32(int32_t value, std::string& out) {
  normalize_uint32(static_cast<uint32_t>(value) ^ 0x80000000u, out);
}

void normalize_int64(int64_t value, std::string& out) {
  append_uint64_be(static_cast<uint64_t>(value) ^ 0x8000000000000000ULL, out);
}

void normalize_float(float value, std::string& out) {
  // Port of DuckDB Radix::EncodeFloat: one canonical zero, infinities at the
  // domain endpoints, and every NaN at the same maximum key.
  uint32_t encoded = 0;
  if (value == 0.0f) {
    encoded = uint32_t{1} << 31;
  } else if (std::isnan(value)) {
    encoded = std::numeric_limits<uint32_t>::max();
  } else if (value > std::numeric_limits<float>::max()) {
    encoded = std::numeric_limits<uint32_t>::max() - 1;
  } else if (value < -std::numeric_limits<float>::max()) {
    encoded = 0;
  } else {
    std::memcpy(&encoded, &value, sizeof(encoded));
    if ((encoded & (uint32_t{1} << 31)) == 0) {
      encoded |= uint32_t{1} << 31;
    } else {
      encoded = ~encoded;
    }
  }
  normalize_uint32(encoded, out);
}

void normalize_double(double value, std::string& out) {
  // Port of DuckDB Radix::EncodeDouble; see normalize_float above.
  uint64_t encoded = 0;
  if (value == 0.0) {
    encoded = uint64_t{1} << 63;
  } else if (std::isnan(value)) {
    encoded = std::numeric_limits<uint64_t>::max();
  } else if (value > std::numeric_limits<double>::max()) {
    encoded = std::numeric_limits<uint64_t>::max() - 1;
  } else if (value < -std::numeric_limits<double>::max()) {
    encoded = 0;
  } else {
    std::memcpy(&encoded, &value, sizeof(encoded));
    if ((encoded & (uint64_t{1} << 63)) == 0) {
      encoded |= uint64_t{1} << 63;
    } else {
      encoded = ~encoded;
    }
  }
  append_uint64_be(encoded, out);
}

void normalize_string_delimited(const std::string& value, std::string& out) {
  // OrderedCode/FoundationDB-style escaping. A data 0x00 becomes 0x00 0xFF;
  // 0x00 0x01 is therefore an unambiguous end-of-column marker.
  for (unsigned char byte : value) {
    if (byte == 0x00) {
      out.push_back(static_cast<char>(0x00));
      out.push_back(static_cast<char>(0xFF));
    } else {
      out.push_back(static_cast<char>(byte));
    }
  }
  out.push_back(static_cast<char>(0x00));
  out.push_back(static_cast<char>(0x01));
}

void normalize_null(std::string& out) {
  out.push_back(NORMALIZED_NULL_MARKER);
}

}  // namespace hyrise::dv_tree
