#include "storage/dependency_validation/hyrise_value_normalization.hpp"

#include <cstddef>

#include <boost/variant/get.hpp>

#include "storage/dependency_validation/key_normalization.hpp"
#include "utils/assert.hpp"

namespace hyrise::dv_tree {

void normalize_all_type_variant(const AllTypeVariant& value, DataType column_type, std::string& out) {
  if (variant_is_null(value)) {
    normalize_null(out);
    return;
  }

  Assert(data_type_from_all_type_variant(value) == column_type,
         "AllTypeVariant does not hold the declared dependency column's data type.");

  out.push_back(NORMALIZED_VALID_MARKER);
  switch (column_type) {
    case DataType::Int:
      normalize_int32(boost::get<int32_t>(value), out);
      return;
    case DataType::Long:
      normalize_int64(boost::get<int64_t>(value), out);
      return;
    case DataType::Float:
      normalize_float(boost::get<float>(value), out);
      return;
    case DataType::Double:
      normalize_double(boost::get<double>(value), out);
      return;
    case DataType::String: {
      // pmr_string cannot bind to key_normalization.hpp's std::string
      // parameter directly (different allocator). Copying by explicit length,
      // not via a NUL-terminated conversion, preserves embedded 0x00/0xFF
      // bytes exactly.
      const auto& string_value = boost::get<pmr_string>(value);
      normalize_string_delimited(std::string(string_value.data(), string_value.size()), out);
      return;
    }
    case DataType::Null:
      Fail("DataType::Null cannot describe a non-NULL AllTypeVariant.");
      break;
  }
}

std::string normalize_dependency_key(const std::vector<AllTypeVariant>& values,
                                      const std::vector<DataType>& column_types) {
  Assert(!values.empty(), "A dependency key needs at least one column.");
  Assert(values.size() == column_types.size(), "Value count must match declared column type count.");

  std::string out;
  for (auto column_index = std::size_t{0}; column_index < values.size(); ++column_index) {
    normalize_all_type_variant(values[column_index], column_types[column_index], out);
  }
  return out;
}

}  // namespace hyrise::dv_tree
