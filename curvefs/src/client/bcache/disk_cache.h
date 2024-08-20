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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_H_
#define CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_H_

#include <atomic>
#include <string>
#include <memory>

#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/bcache/error.h"
#include "curvefs/src/client/bcache/cache_store.h"
#include "curvefs/src/client/bcache/local_filesystem.h"
#include "curvefs/src/client/bcache/disk_cache_layout.h"
#include "curvefs/src/client/bcache/disk_cache_manager.h"
#include "curvefs/src/client/bcache/disk_cache_loader.h"
#include "curvefs/src/client/bcache/metric.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curvefs::client::common::DiskCacheOption;

class DiskCache : public CacheStore {
    enum : uint8_t {
        WANT_EXEC = 1,
        WANT_STAGE = 2,
        WANT_CACHE = 4,
    };

    enum class CacheStatus {
        UP,
        DOWN,
    };

 public:
    explicit DiskCache(DiskCacheOption option);

    BCACHE_ERROR Init(UploadFunc uploader) override;

    BCACHE_ERROR Shutdown() override;

    BCACHE_ERROR Stage(const BlockKey& key, const Block& block) override;

    BCACHE_ERROR RemoveStage(const BlockKey& key) override;

    BCACHE_ERROR Cache(const BlockKey& key, const Block& block) override;

    BCACHE_ERROR Load(const BlockKey& key, Block* block) override;

 private:
    // check running status, disk healthy and disk free space
    BCACHE_ERROR Check(uint8_t want);

    bool IsCached(const BlockKey& key);

    std::string GetRootDir() const;

    std::string GetStagePath(const BlockKey& key) const;

    std::string GetCachePath(const BlockKey& key) const;

    bool Loading() const;

    bool IsHealthy() const;

    bool StageFull() const;

    bool CacheFull() const;

 private:
    UploadFunc uploader_;
    DiskCacheOption option_;
    std::atomic<CacheStatus> status_;
    std::unique_ptr<LocalFilesystem> fs_;
    std::shared_ptr<DiskCacheLayout> layout_;
    std::shared_ptr<DiskCacheManager> manager_;
    std::unique_ptr<DiskCacheLoader> loader_;
    std::shared_ptr<DiskCacheMetric> metric_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_DISK_CACHE_H_
