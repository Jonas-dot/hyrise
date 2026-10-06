#include "storage/dependency_validation/dependency_validation_staging.hpp"

#include <memory>
#include <vector>

#include "concurrency/transaction_context.hpp"
#include "storage/chunk.hpp"
#include "storage/dependency_validation/hyrise_value_normalization.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace hyrise::dv_tree {

void stage_dependency_row(const std::shared_ptr<TransactionContext>& context, const std::shared_ptr<const Table>& table,
                          const std::shared_ptr<const Chunk>& chunk, const ChunkOffset chunk_offset,
                          const bool is_delete) {
  Assert(context, "Dependency staging needs a transaction context.");
  Assert(table, "Dependency staging needs a table.");
  Assert(chunk, "Dependency staging needs a chunk.");

  // Delete and Update may receive a reference table produced by Validate and
  // TableScan rather than the physical stored table. Follow its first-column
  // reference chain to the actual row before looking up descriptors or reading
  // values. A ReferenceSegment's position list maps this chunk-local offset to
  // the next table's RowID; a physical table has a non-reference first segment.
  auto physical_table = table;
  auto physical_chunk = chunk;
  auto physical_offset = chunk_offset;
  while (const auto reference =
             std::dynamic_pointer_cast<const ReferenceSegment>(physical_chunk->get_segment(ColumnID{0}))) {
    const auto row_id = (*reference->pos_list())[physical_offset];
    physical_table = reference->referenced_table();
    physical_chunk = physical_table->get_chunk(row_id.chunk_id);
    Assert(physical_chunk, "A dependency row reference must resolve to an existing chunk.");
    physical_offset = row_id.chunk_offset;
  }

  const auto& dependencies = physical_table->_validation_dependencies;
  if (dependencies.empty())
    return;

  for (const auto& dependency : dependencies) {
    auto lhs_values = std::vector<AllTypeVariant>{};
    auto rhs_values = std::vector<AllTypeVariant>{};
    lhs_values.reserve(dependency.lhs_column_ids.size());
    rhs_values.reserve(dependency.rhs_column_ids.size());

    for (const auto column_id : dependency.lhs_column_ids) {
      lhs_values.emplace_back((*physical_chunk->get_segment(column_id))[physical_offset]);
    }
    for (const auto column_id : dependency.rhs_column_ids) {
      rhs_values.emplace_back((*physical_chunk->get_segment(column_id))[physical_offset]);
    }

    auto lhs_norm = normalize_dependency_key(lhs_values, dependency.lhs_column_types);
    auto rhs_norm = normalize_dependency_key(rhs_values, dependency.rhs_column_types);
    if (is_delete) {
      context->stage_dependency_remove(physical_table, dependency.dv_tree, dependency.name, std::move(lhs_norm),
                                       std::move(rhs_norm));
    } else {
      context->stage_dependency_insert(physical_table, dependency.dv_tree, dependency.name, std::move(lhs_norm),
                                       std::move(rhs_norm));
    }
  }
}

}  // namespace hyrise::dv_tree
