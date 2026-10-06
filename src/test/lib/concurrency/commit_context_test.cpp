#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "concurrency/commit_context.hpp"
#include "types.hpp"

namespace hyrise {

class CommitContextTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(CommitContextTest, HasNextReturnsFalse) {
  auto context = std::make_unique<CommitContext>(CommitID{0});

  EXPECT_FALSE(context->has_next());
}

TEST_F(CommitContextTest, HasNextReturnsTrueAfterNextHasBeenSet) {
  auto context = std::make_unique<CommitContext>(CommitID{0});

  auto next_context = std::make_shared<CommitContext>(CommitID{context->commit_id() + 1});

  EXPECT_TRUE(context->try_set_next(next_context));

  EXPECT_TRUE(context->has_next());
}

TEST_F(CommitContextTest, TrySetNextFailsIfNotNullptr) {
  auto context = std::make_unique<CommitContext>(CommitID{0});

  auto next_context = std::make_shared<CommitContext>(CommitID{context->commit_id() + 1});

  EXPECT_TRUE(context->try_set_next(next_context));

  next_context = std::make_shared<CommitContext>(CommitID{context->commit_id() + 1});

  EXPECT_FALSE(context->try_set_next(next_context));
}

TEST_F(CommitContextTest, PrepublicationCallbackFiresExactlyOnceBeforeCommitCallback) {
  auto context = std::make_unique<CommitContext>(CommitID{1});
  auto events = std::vector<std::string>{};

  context->make_pending(
      TransactionID{1},
      [&events](const TransactionID /*transaction_id*/) {
        events.emplace_back("committed");
      },
      [&events] {
        events.emplace_back("prepublication");
      });

  context->fire_prepublication_callback();
  context->fire_prepublication_callback();
  context->fire_callback();

  EXPECT_EQ(events, (std::vector<std::string>{"prepublication", "committed"}));
}

}  // namespace hyrise
