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
 * Created Date: 2024-08-26
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCK_CACHE_NONE_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCK_CACHE_NONE_CACHE_H_

#include "curvefs/src/client/block_cache/cache_store.h"
#include "curvefs/src/client/block_cache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

class NoneCache : public CacheStore {
 public:
  NoneCache() = default;

  ~NoneCache() = default;

  BCACHE_ERROR Init(CacheStore::UploadFunc uploader) override {
    return BCACHE_ERROR::OK;
  }

  BCACHE_ERROR Shutdown() override { return BCACHE_ERROR::OK; }

  BCACHE_ERROR Stage(const BlockKey& key, const Block& block) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR RemoveStage(const BlockKey& key) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  BCACHE_ERROR Load(const BlockKey& key,
                    std::shared_ptr<BlockReader>& reader) override {
    return BCACHE_ERROR::NOT_SUPPORTED;
  }

  bool IsCached(const BlockKey& key) override { return false; }
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_CACHE_NONE_CACHE_H_
