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
 * Created Date: 2024-09-02
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_

#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "curvefs/src/base/cache/cache.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/blockcache/cache_store.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::cache::Cache;
using ::curvefs::base::time::TimeSpec;

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

enum class EvictStatus {
  EVICT_SKIP,
  EVICT_OK,
  EVICT_DONE,
};

// protect by DiskCacheManager
class LRUCache {
 public:
  struct ListNode {
    CacheValue* value;
    Cache::Handle* handle;
    struct ListNode* prev;
    struct ListNode* next;
  };

  using EvictFunc = std::function<EvictStatus(const CacheValue& value)>;

 public:
  LRUCache();

  ~LRUCache();

  void Add(const CacheKey& key, const CacheValue& value);

  bool Get(const CacheKey& key, CacheValue* value);

  std::shared_ptr<CacheItems> Evict(EvictFunc func);

  size_t Size();

 private:
  static void DeleteNode(const std::string_view& key, void* value);

  void AddFront(ListNode* list, ListNode* node);

  void Remove(ListNode* node);

 private:
  size_t size_;
  ListNode active_;
  ListNode inactive_;
  static CacheItems evicted_;
  std::unique_ptr<Cache> cache_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_LRU_CACHE_H_
