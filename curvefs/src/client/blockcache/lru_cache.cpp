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

#include <cassert>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/time.h"
#include "curvefs/src/client/blockcache/lru_cache.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::absl::MakeCleanup;
using ::curvefs::base::time::Now;
using ::curvefs::base::cache::Cache;
using ::curvefs::base::cache::NewLRUCache;

LRUCache::LRUCache()
  :size_(0) {
  cache_ = std::unique_ptr<Cache>(NewLRUCache(1 << 30));  // large enough
  inactive_.next = &inactive_;
  inactive_.prev = &inactive_;
  active_.next = &active_;
  active_.prev = &active_;
}

void LRUCache::Add(const CacheKey& key, const CacheValue& value) {
  ListNode* node = new ListNode();
  node->value = new CacheValue(std::move(value));

  auto handle = cache_->Insert(key.Filename(), node, 1, &LRUCache::DeleteNode);
  auto defer = MakeCleanup([handle, this]() { cache_->Release(handle); });
  node->handle = handle;
  size_++;
  AddFront(&inactive_, node);
}

bool LRUCache::Get(const CacheKey& key, CacheValue* value) {
  auto handle = cache_->Lookup(key.Filename());
  if (nullptr == handle) {
    return false;
  }

  auto defer = MakeCleanup([handle, this]() { cache_->Release(handle); });
  ListNode* node = reinterpret_cast<ListNode*>(cache_->Value(handle));
  Remove(node);
  AddFront(&active_, node);
  node->value->atime = Now();
  *value = *node->value;
  return true;
}

std::shared_ptr<CacheItems> LRUCache::Evict(EvictFunc func) {
  bool done = false;
  evicted_.clear();
  std::vector<ListNode> lists{ inactive_, active_ };
  for (const auto& list : lists) {
    for (ListNode* node = list.next; node != &list; ) {
      auto rc = func(*node->value);
      if (rc == EvictCode::EVICT_OK) {
        cache_->Release(node->handle);
        size_--;
      } else if (rc == EvictCode::EVICT_SKIP) {
        continue;
      } else {   // done
        done = true;
        break;
      }
    }

    if (done) {
      break;
    }
  }
  return std::make_shared<CacheItems>(std::move(evicted_));
}

void LRUCache::AddFront(ListNode* list, ListNode* node) {
  node->next = list;
  node->prev = list->prev;
  node->prev->next = node;
  node->next->prev = node;
}

void LRUCache::Remove(ListNode* node) {
  node->next->prev = node->prev;
  node->prev->next = node->next;
}

void LRUCache::DeleteNode(const std::string_view& key, void* value) {
  ListNode* node = reinterpret_cast<ListNode*>(value);

  CacheKey k;
  CacheValue v = std::move(*node->value);
  assert(k.ParseFilename(key));
  evicted_.emplace_back(CacheItem(k, v));

  delete node->value;
  delete node;
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
