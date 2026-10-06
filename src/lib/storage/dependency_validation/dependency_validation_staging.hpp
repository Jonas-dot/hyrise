#pragma once

#include <memory>

#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;
class TransactionContext;

namespace dv_tree {

// Materializes one row's declared dependency tuples, normalizes them, and
// appends them to the active transaction's private DV write set. This is used
// only while DML operators execute; it does not reserve a CID or mutate a
// DVTree. Any materialization/normalization error is therefore still an
// ordinary operator failure and can safely trigger rollback.
void stage_dependency_row(const std::shared_ptr<TransactionContext>& context, const std::shared_ptr<const Table>& table,
                          const std::shared_ptr<const Chunk>& chunk, ChunkOffset chunk_offset, bool is_delete);

}  // namespace dv_tree
}  // namespace hyrise
