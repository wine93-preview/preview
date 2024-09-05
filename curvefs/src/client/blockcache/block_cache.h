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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/s3_client.h"
#include "curvefs/src/client/common/config.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::TaskThreadPool;
using ::curvefs::client::common::BlockCacheOption;

class BlockCache {
 public:
  enum class StoreType {
    NONE,
    DISK,
  };

  using DoFunc =
      std::function<BCACHE_ERROR(const std::shared_ptr<CacheStore>&)>;

 public:
  virtual BCACHE_ERROR Init() = 0;

  virtual BCACHE_ERROR Shutdown() = 0;

  virtual BCACHE_ERROR Put(const BlockKey& key, const Block& block) = 0;

  virtual BCACHE_ERROR Range(const BlockKey& key, off_t offset, size_t size,
                             char* buffer, bool retrive = true) = 0;

  virtual BCACHE_ERROR Cache(const BlockKey& key, const Block& block) = 0;

  virtual BCACHE_ERROR Flush(uint64_t ino) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual StoreType GetStoreType() = 0;
};

class StageBlockCount {
  struct Value {
    Value() : count(0), cond(std::make_shared<std::condition_variable>()){};

    int64_t count;
    std::shared_ptr<std::condition_variable> cond;
  };

 public:
  StageBlockCount();

  void Add(uint64_t ino, int64_t n);

  void Wait(uint64_t ino);

  bool Empty();

 private:
  std::mutex mutex_;
  std::unordered_map<uint64_t, Value> pending_;
};

class BlockCacheImpl : public BlockCache {
 public:
  explicit BlockCacheImpl(BlockCacheOption option);

  ~BlockCacheImpl() = default;

  BCACHE_ERROR Init() override;

  BCACHE_ERROR Shutdown() override;

  BCACHE_ERROR Put(const BlockKey& key, const Block& block) override;

  BCACHE_ERROR Range(const BlockKey& key, off_t offset, size_t size,
                     char* buffer, bool retrive = true) override;

  BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override;

  BCACHE_ERROR Flush(uint64_t ino) override;

  bool IsCached(const BlockKey& key) override;

  StoreType GetStoreType() override;

 private:
  void UploadStagingFile(const BlockKey& key, const std::string& path,
                         bool reload);

  void WaitUploadFinish();

 private:
  BlockCacheOption option_;
  std::atomic<bool> running_;
  std::shared_ptr<S3Client> s3_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<StageBlockCount> staging_;
  std::unique_ptr<TaskThreadPool<>> uploading_;
};

inline BlockCacheImpl::StoreType BlockCacheImpl::GetStoreType() {
  if (option_.cacheStore == "none") {
    return StoreType::NONE;
  }
  return StoreType::DISK;
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_H_
