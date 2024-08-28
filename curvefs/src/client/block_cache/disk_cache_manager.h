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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "curvefs/src/base/cache.h"
#include "curvefs/src/base/message_queue.h"
#include "curvefs/src/base/time.h"
#include "curvefs/src/client/block_cache/cache_store.h"
#include "curvefs/src/client/block_cache/disk_cache_layout.h"
#include "curvefs/src/client/block_cache/local_filesystem.h"
#include "src/common/interruptible_sleeper.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::InterruptibleSleeper;
using ::curve::common::Mutex;
using ::curvefs::base::cache::Cache;
using ::curvefs::base::cache::NewLRUCache;
using ::curvefs::base::queue::MessageQueue;
using ::curvefs::base::time::TimeSpec;

// Manage cache items and its capacity
class DiskCacheManager {
 public:
  using CacheKey = BlockKey;

  struct CacheValue {
    CacheValue() = default;

    CacheValue(size_t size, TimeSpec atime) : size(size), atime(atime) {}

    size_t size;
    TimeSpec atime;
  };

  struct CacheItem {
    CacheItem(CacheKey key, CacheValue value) : key(key), value(value) {}

    CacheKey key;
    CacheValue value;
  };

  using CacheItems = std::vector<CacheItem>;
  using MessageType = std::shared_ptr<CacheItems>;
  using MessageQueueType = MessageQueue<MessageType>;

 public:
  DiskCacheManager(uint64_t capacity, double freeSpaceRatio,
                   std::shared_ptr<LocalFileSystem> fs,
                   std::shared_ptr<DiskCacheLayout> layout);

  void Start();

  void Stop();

  void Add(const CacheKey& key, const CacheValue& value);

  BCACHE_ERROR Get(const CacheKey& key, CacheValue* value);

  bool StageFull() const;

  bool CacheFull() const;

 private:
  void CheckFreeSpace();

  void CleanupFull(uint64_t goalBytes, uint64_t goalFiles);

  void CleanupExpire();

  void DeleteBlocks(const std::shared_ptr<CacheItems>& toDel);

  std::string GetCachePath(const CacheKey& key);

 private:
  Mutex mutex_;  // capacity releated
  uint64_t usedBytes_;
  uint64_t capacity_;
  double freeSpaceRatio_;
  std::atomic<bool> stageFull_;
  std::atomic<bool> cacheFull_;
  std::thread thread_;  // thread releated
  std::atomic<bool> running_;
  InterruptibleSleeper sleeper_;
  std::shared_ptr<LocalFileSystem> fs_;  // other members
  std::shared_ptr<DiskCacheLayout> layout_;
  std::unique_ptr<MessageQueueType> mq_;
  std::unique_ptr<Cache> cache_;
  std::vector<Cache::Handle*> handles_;
};

inline bool DiskCacheManager::StageFull() const {
  return stageFull_.load(std::memory_order_acquire);
}

inline bool DiskCacheManager::CacheFull() const {
  return cacheFull_.load(std::memory_order_acquire);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_MANAGER_H_
