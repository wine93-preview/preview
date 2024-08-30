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

#include "curvefs/src/client/block_cache/disk_cache_group.h"

#include <map>

namespace curvefs {
namespace client {
namespace blockcache {

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : options_(options) {}

BCACHE_ERROR DiskCacheGroup::Init(CacheStore::UploadFunc uploader) {
  for (const auto& option : options_) {
    auto store = std::make_shared<DiskCache>(option);
    auto rc = store->Init(uploader);
    if (rc != BCACHE_ERROR::OK) {
      return rc;
    }
    stores_.emplace_back(store);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCacheGroup::Shutdown() {
  for (const auto& store : stores_) {
    auto rc = store->Shutdown();
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
  return stores_[key.id % stores_.size()];  // TODO: chash
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
