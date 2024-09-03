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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/disk_cache_group.h"

#include <map>
#include <cassert>

#include "curvefs/src/base/ketama_con_hash.h"
#include "curvefs/src/base/math.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::hash::ConNode;
using ::curvefs::base::hash::KetamaConHash;
using ::curvefs::base::math::Gcd;

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : options_(options), chash_(std::make_unique<KetamaConHash>()) {}

BCACHE_ERROR DiskCacheGroup::Init(CacheStore::UploadFunc uploader) {
  uint64_t gcd = 0;
  std::for_each(
      options_.begin(), options_.end(),
      [&](const DiskCacheOption& option) { gcd = Gcd(gcd, option.cacheSize); });
  assert(gcd != 0);

  for (const auto& option : options_) {
    auto store = std::make_shared<DiskCache>(option);
    auto rc = store->Init(uploader);
    if (rc != BCACHE_ERROR::OK) {
      return rc;
    }
    chash_->AddNode(store->Id(), option.cacheSize / gcd);
    stores_[store->Id()] = store;
  }
  chash_->Final();
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheGroup::Shutdown() {
  for (const auto& it : stores_) {
    auto rc = it.second->Shutdown();
    if (rc != BCACHE_ERROR::OK) {
      return rc;
    }
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheGroup::Stage(const BlockKey& key, const Block& block) {
  return GetStore(key)->Stage(key, block);
}

BCACHE_ERROR DiskCacheGroup::RemoveStage(const BlockKey& key) {
  return GetStore(key)->RemoveStage(key);
}

BCACHE_ERROR DiskCacheGroup::Cache(const BlockKey& key, const Block& block) {
  return GetStore(key)->Cache(key, block);
}

BCACHE_ERROR DiskCacheGroup::Load(const BlockKey& key,
                                  std::shared_ptr<BlockReader>& reader) {
  return GetStore(key)->Load(key, reader);
}

bool DiskCacheGroup::IsCached(const BlockKey& key) {
  return GetStore(key)->IsCached(key);
}

std::shared_ptr<DiskCache> DiskCacheGroup::GetStore(const BlockKey& key) {
  ConNode node;
  bool find = chash_->Lookup(std::to_string(key.id), node);
  assert(find);

  auto it = stores_.find(node.key);
  assert(it != stores_.end());
  return it->second;
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
