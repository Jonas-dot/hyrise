#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "storage/dependency_validation/hyrise_value_normalization.hpp"
#include "storage/dependency_validation/key_normalization.hpp"

namespace hyrise::dv_tree {

class HyriseValueNormalizationTest : public BaseTest {};

// Phase 4 exit criterion: the same logical key produces identical bytes
// whether it is built through the standalone core primitives directly or
// through this Hyrise-facing adapter. Each check below reimplements the
// expected bytes using key_normalization.hpp's own functions and compares
// against the adapter's output, rather than hardcoding byte literals.

TEST_F(HyriseValueNormalizationTest, MatchesCoreForEveryScalarType) {
  {
    const auto variant = AllTypeVariant{int32_t{-17}};
    auto actual = std::string{};
    normalize_all_type_variant(variant, DataType::Int, actual);

    auto expected = std::string{};
    expected.push_back(NORMALIZED_VALID_MARKER);
    normalize_int32(-17, expected);
    EXPECT_EQ(actual, expected);
  }
  {
    const auto variant = AllTypeVariant{int64_t{-1234567890123}};
    auto actual = std::string{};
    normalize_all_type_variant(variant, DataType::Long, actual);

    auto expected = std::string{};
    expected.push_back(NORMALIZED_VALID_MARKER);
    normalize_int64(-1234567890123, expected);
    EXPECT_EQ(actual, expected);
  }
  {
    const auto variant = AllTypeVariant{float{3.5F}};
    auto actual = std::string{};
    normalize_all_type_variant(variant, DataType::Float, actual);

    auto expected = std::string{};
    expected.push_back(NORMALIZED_VALID_MARKER);
    normalize_float(3.5F, expected);
    EXPECT_EQ(actual, expected);
  }
  {
    const auto variant = AllTypeVariant{double{-2.25}};
    auto actual = std::string{};
    normalize_all_type_variant(variant, DataType::Double, actual);

    auto expected = std::string{};
    expected.push_back(NORMALIZED_VALID_MARKER);
    normalize_double(-2.25, expected);
    EXPECT_EQ(actual, expected);
  }
  {
    const auto variant = AllTypeVariant{pmr_string{"hyrise"}};
    auto actual = std::string{};
    normalize_all_type_variant(variant, DataType::String, actual);

    auto expected = std::string{};
    expected.push_back(NORMALIZED_VALID_MARKER);
    normalize_string_delimited(std::string("hyrise"), expected);
    EXPECT_EQ(actual, expected);
  }
  {
    auto actual = std::string{};
    normalize_all_type_variant(NULL_VALUE, DataType::Int, actual);

    auto expected = std::string{};
    normalize_null(expected);
    EXPECT_EQ(actual, expected);
  }
}

TEST_F(HyriseValueNormalizationTest, RejectsVariantColumnTypeMismatch) {
  const auto variant = AllTypeVariant{int32_t{1}};
  auto out = std::string{};
  EXPECT_THROW(normalize_all_type_variant(variant, DataType::Long, out), std::logic_error);
}

TEST_F(HyriseValueNormalizationTest, NullAlwaysOrdersAfterNonNullInTheSameColumn) {
  auto null_bytes = std::string{};
  normalize_all_type_variant(NULL_VALUE, DataType::Int, null_bytes);

  for (const auto value : {std::numeric_limits<int32_t>::min(), int32_t{-1}, int32_t{0}, int32_t{1},
                           std::numeric_limits<int32_t>::max()}) {
    auto value_bytes = std::string{};
    normalize_all_type_variant(AllTypeVariant{value}, DataType::Int, value_bytes);
    EXPECT_LT(value_bytes, null_bytes) << "value=" << value;
  }
}

TEST_F(HyriseValueNormalizationTest, IntegerOrderIsPreserved) {
  const auto values = std::vector<int32_t>{
      std::numeric_limits<int32_t>::min(), -1000, -1, 0, 1, 1000, std::numeric_limits<int32_t>::max()};
  for (auto i = std::size_t{0}; i + 1 < values.size(); ++i) {
    auto lower = std::string{};
    auto upper = std::string{};
    normalize_all_type_variant(AllTypeVariant{values[i]}, DataType::Int, lower);
    normalize_all_type_variant(AllTypeVariant{values[i + 1]}, DataType::Int, upper);
    EXPECT_LT(lower, upper) << values[i] << " vs " << values[i + 1];
  }
}

TEST_F(HyriseValueNormalizationTest, LongOrderIsPreserved) {
  const auto values = std::vector<int64_t>{std::numeric_limits<int64_t>::min(),
                                            int64_t{-1000000000000},
                                            int64_t{-1},
                                            int64_t{0},
                                            int64_t{1},
                                            int64_t{1000000000000},
                                            std::numeric_limits<int64_t>::max()};
  for (auto i = std::size_t{0}; i + 1 < values.size(); ++i) {
    auto lower = std::string{};
    auto upper = std::string{};
    normalize_all_type_variant(AllTypeVariant{values[i]}, DataType::Long, lower);
    normalize_all_type_variant(AllTypeVariant{values[i + 1]}, DataType::Long, upper);
    EXPECT_LT(lower, upper) << values[i] << " vs " << values[i + 1];
  }
}

TEST_F(HyriseValueNormalizationTest, FloatingPointOrderAndCanonicalEdgeCasesArePreserved) {
  // -0.0 is deliberately omitted from this strictly-increasing list: it
  // canonicalizes to the identical key as 0.0 (checked separately below), so
  // adjacent EXPECT_LT would spuriously fail on that pair.
  const auto values = std::vector<double>{-std::numeric_limits<double>::infinity(),
                                           std::numeric_limits<double>::lowest(),
                                           -1.5,
                                           0.0,
                                           1.5,
                                           std::numeric_limits<double>::max(),
                                           std::numeric_limits<double>::infinity()};
  for (auto i = std::size_t{0}; i + 1 < values.size(); ++i) {
    auto lower = std::string{};
    auto upper = std::string{};
    normalize_all_type_variant(AllTypeVariant{values[i]}, DataType::Double, lower);
    normalize_all_type_variant(AllTypeVariant{values[i + 1]}, DataType::Double, upper);
    EXPECT_LT(lower, upper) << values[i] << " vs " << values[i + 1];
  }

  // -0.0 and +0.0 canonicalize to the identical byte string (literal equality).
  auto negative_zero = std::string{};
  auto positive_zero = std::string{};
  normalize_all_type_variant(AllTypeVariant{-0.0}, DataType::Double, negative_zero);
  normalize_all_type_variant(AllTypeVariant{0.0}, DataType::Double, positive_zero);
  EXPECT_EQ(negative_zero, positive_zero);

  // Every NaN payload canonicalizes to the same key, ordered after +infinity.
  auto nan_a = std::string{};
  auto nan_b = std::string{};
  auto positive_infinity = std::string{};
  normalize_all_type_variant(AllTypeVariant{std::numeric_limits<double>::quiet_NaN()}, DataType::Double, nan_a);
  normalize_all_type_variant(AllTypeVariant{-std::numeric_limits<double>::quiet_NaN()}, DataType::Double, nan_b);
  normalize_all_type_variant(AllTypeVariant{std::numeric_limits<double>::infinity()}, DataType::Double,
                              positive_infinity);
  EXPECT_EQ(nan_a, nan_b);
  EXPECT_GT(nan_a, positive_infinity);
}

TEST_F(HyriseValueNormalizationTest, FloatOrderAndCanonicalEdgeCasesArePreserved) {
  // Mirrors the double test above at float precision: same -0.0/0.0 caveat.
  const auto values = std::vector<float>{-std::numeric_limits<float>::infinity(),
                                          std::numeric_limits<float>::lowest(),
                                          -1.5F,
                                          0.0F,
                                          1.5F,
                                          std::numeric_limits<float>::max(),
                                          std::numeric_limits<float>::infinity()};
  for (auto i = std::size_t{0}; i + 1 < values.size(); ++i) {
    auto lower = std::string{};
    auto upper = std::string{};
    normalize_all_type_variant(AllTypeVariant{values[i]}, DataType::Float, lower);
    normalize_all_type_variant(AllTypeVariant{values[i + 1]}, DataType::Float, upper);
    EXPECT_LT(lower, upper) << values[i] << " vs " << values[i + 1];
  }

  auto negative_zero = std::string{};
  auto positive_zero = std::string{};
  normalize_all_type_variant(AllTypeVariant{-0.0F}, DataType::Float, negative_zero);
  normalize_all_type_variant(AllTypeVariant{0.0F}, DataType::Float, positive_zero);
  EXPECT_EQ(negative_zero, positive_zero);

  auto nan_a = std::string{};
  auto nan_b = std::string{};
  auto positive_infinity = std::string{};
  normalize_all_type_variant(AllTypeVariant{std::numeric_limits<float>::quiet_NaN()}, DataType::Float, nan_a);
  normalize_all_type_variant(AllTypeVariant{-std::numeric_limits<float>::quiet_NaN()}, DataType::Float, nan_b);
  normalize_all_type_variant(AllTypeVariant{std::numeric_limits<float>::infinity()}, DataType::Float,
                              positive_infinity);
  EXPECT_EQ(nan_a, nan_b);
  EXPECT_GT(nan_a, positive_infinity);
}

TEST_F(HyriseValueNormalizationTest, StringOrderIsPreservedIncludingEmbeddedZeroAndFF) {
  // pmr_string has no converting constructor from std::string (different
  // allocator types), so embedded 0x00/0xFF bytes are built via the explicit
  // (data, length) constructor instead of string concatenation.
  const auto values = std::vector<pmr_string>{
      pmr_string{""},
      pmr_string{"a"},
      pmr_string("a\0b", 3),
      pmr_string{"ab"},
      pmr_string("ab\xFF", 3),
      pmr_string{"b"},
  };
  for (auto i = std::size_t{0}; i + 1 < values.size(); ++i) {
    auto lower = std::string{};
    auto upper = std::string{};
    normalize_all_type_variant(AllTypeVariant{values[i]}, DataType::String, lower);
    normalize_all_type_variant(AllTypeVariant{values[i + 1]}, DataType::String, upper);
    EXPECT_LT(lower, upper) << "index " << i;
  }
}

TEST_F(HyriseValueNormalizationTest, CompositeColumnsAreSelfDelimiting) {
  // ("a", "bc") must not collide with ("ab", "c"): normalize_string_delimited
  // escapes embedded 0x00 bytes, so concatenation cannot confuse the column
  // boundary.
  const auto key_a_bc =
      normalize_dependency_key({AllTypeVariant{pmr_string{"a"}}, AllTypeVariant{pmr_string{"bc"}}},
                                {DataType::String, DataType::String});
  const auto key_ab_c =
      normalize_dependency_key({AllTypeVariant{pmr_string{"ab"}}, AllTypeVariant{pmr_string{"c"}}},
                                {DataType::String, DataType::String});
  EXPECT_NE(key_a_bc, key_ab_c);
}

TEST_F(HyriseValueNormalizationTest, CompositeKeyHandlesNullInAnyPosition) {
  const auto both_present =
      normalize_dependency_key({AllTypeVariant{int32_t{1}}, AllTypeVariant{pmr_string{"x"}}},
                                {DataType::Int, DataType::String});
  const auto lhs_null =
      normalize_dependency_key({NULL_VALUE, AllTypeVariant{pmr_string{"x"}}}, {DataType::Int, DataType::String});
  const auto rhs_null =
      normalize_dependency_key({AllTypeVariant{int32_t{1}}, NULL_VALUE}, {DataType::Int, DataType::String});
  const auto both_null = normalize_dependency_key({NULL_VALUE, NULL_VALUE}, {DataType::Int, DataType::String});

  EXPECT_NE(both_present, lhs_null);
  EXPECT_NE(both_present, rhs_null);
  EXPECT_NE(lhs_null, rhs_null);
  EXPECT_NE(both_null, lhs_null);
  EXPECT_NE(both_null, rhs_null);

  // Repeated normalization of the identical logical tuple is byte-identical.
  const auto both_null_again = normalize_dependency_key({NULL_VALUE, NULL_VALUE}, {DataType::Int, DataType::String});
  EXPECT_EQ(both_null, both_null_again);
}

}  // namespace hyrise::dv_tree
