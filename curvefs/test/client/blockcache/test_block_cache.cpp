/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/client/blockcache/block_cache.h"
#include "curvefs/test/client/blockcache/helper/builder.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::absl::MakeCleanup;

class BlockCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BlockCacheTest, Basic) {
  auto builder = BlockCacheBuilder();
  auto defer = MakeCleanup([&]() { builder.Cleanup(); });

  auto block_cache = builder.Build();
  ASSERT_EQ(block_cache->Init(), BCACHE_ERROR::OK);
  ASSERT_EQ(block_cache->GetStoreType(), StoreType::DISK);
  ASSERT_EQ(block_cache->Shutdown(), BCACHE_ERROR::OK);
}

TEST_F(BlockCacheTest, Put) {
  auto builder = BlockCacheBuilder();
  auto block_cache = builder.Build();
  ASSERT_EQ(block_cache->Init(), BCACHE_ERROR::OK);
  auto defer = MakeCleanup([&]() {
    block_cache->Shutdown();
    builder.Cleanup();
  });

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build("");
  ASSERT_EQ(block_cache->Put(key, block), BCACHE_ERROR::OK);
  ASSERT_TRUE(block_cache->IsCached(key));

  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto fs = NewTempLocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto stage_path = PathJoin({root_dir, "stage", key.StoreKey()});
  auto cache_path = PathJoin({root_dir, "cache", key.StoreKey()});
  ASSERT_TRUE(fs->FileExists(stage_path));
  ASSERT_TRUE(fs->FileExists(cache_path));
}

TEST_F(BlockCacheTest, Range) {
  auto builder = BlockCacheBuilder();
  auto block_cache = builder.Build();
  ASSERT_EQ(block_cache->Init(), BCACHE_ERROR::OK);
  auto defer = MakeCleanup([&]() {
    block_cache->Shutdown();
    builder.Cleanup();
  });

  auto key = BlockKeyBuilder().Build(100);
  EXPECT_CALL(*builder.GetS3Client(), Range(_, _, _, _))
      .WillOnce(Return(BCACHE_ERROR::OK));
  ASSERT_EQ(block_cache->Range(key, 0, 0, nullptr, true), BCACHE_ERROR::OK);
}

TEST_F(BlockCacheTest, Cache) {
  auto builder = BlockCacheBuilder();
  auto block_cache = builder.Build();
  ASSERT_EQ(block_cache->Init(), BCACHE_ERROR::OK);
  auto defer = MakeCleanup([&]() {
    block_cache->Shutdown();
    builder.Cleanup();
  });

  auto key = BlockKeyBuilder().Build(100);
  auto block = BlockBuilder().Build("");
  ASSERT_EQ(block_cache->Cache(key, block), BCACHE_ERROR::OK);
  ASSERT_TRUE(block_cache->IsCached(key));

  auto fs = NewTempLocalFileSystem();
  auto root_dir = builder.GetRootDir();
  auto cache_path = PathJoin({root_dir, "cache", key.StoreKey()});
  ASSERT_TRUE(fs->FileExists(cache_path));
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
