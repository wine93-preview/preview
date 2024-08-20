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

#include <glog/logging.h>

#include <map>

#include "curvefs/src/base/base.h"
#include "curvefs/src/client/bcache/disk_cache.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curvefs::base::time::Now;

using CacheManager::CacheValue;

DiskCache::DiskCache(DiskCacheOption option)
    : option_(option), status_(CacheStatus::DOWN) {
    fs_ = std::make_shared<LocalFileSystem>();
    layout_ = std::make_shared<DiskCacheLayout>(option.cacheDir);
    manager_ = std::make_shared<DiskCacheManager>(
        option.cacheSize, option.freeSpaceRatio, fs_, layout_);
    loader_ = std::make_unique<DiskCacheLoader>(fs_, layout_, manager_);
    metric_ = std::make_shared<DiskCacheMetric>(option);
}

BCACHE_ERROR DiskCache::Init(UploadFunc uploader) {
    if (status_.exchange(CacheStatus::UP) != CacheStatus::DOWN) {
        uploader_ = uploader;
        manager_->Start();
        loader_->Start(uploader);
        LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is up."
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCache::Shutdown() {
    if (status_.exchange(CacheStatus::DOWN) != CacheStatus::UP) {
        LOG(INFO) << "Disk cache (dir=" << GetRootDir()
                  << ") is shutting down..."
        loader_->Stop();
        manager_->Stop();
        LOG(INFO) << "Disk cache (dir=" << GetRootDir() << ") is down."
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR DiskCache::Stage(const BlockKey& key, const Block& block) {
    auto rc = Check(WANT_EXEC | WANT_STAGE);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    std::string stagePath(GetStagePath(key));
    std::string cachePath(GetCachePath(key));
    rc = fs_->WriteFile(stagePath, block.data, block.size);
    if (rc == BCACHE_ERROR::OK) {
        auto err = fs_->HardLink(stagePath, cachePath);
        if (err == BCACHE_ERROR::OK) {
            manager_->Add(key, CacheValue(block.size, Now()));
        } else {
            LOG(WARN) << "Link " << stagePath << " to " << cachePath
                      << " failed: " << StrErr(err);
        }
    }
    uploader_(key, path);
    return rc;
}

BCACHE_ERROR DiskCache::RemoveStage(const BlockKey& key) {
    auto rc = Check(WANT_EXEC);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }
    return fs_->RemoveFile(GetStagePath(key));
}

BCACHE_ERROR DiskCache::Cache(const BlockKey& key, const Block& block) {
    auto rc = Check(WANT_EXEC | WANT_CACHE);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    rc = fs_->WriteFile(GetCachePath(key), block.data, block.size);
    if (rc == BCACHE_ERROR::OK) {
        manager_->Add(key, CacheValue(block.size, Now()));
    }
    return rc;
}

BCACHE_ERROR DiskCache::Load(const BlockKey& key, Block* block) {
    auto rc = Check(WANT_EXEC);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    } else if (!IsCached(key)) {
        return BCACHE_ERROR::NOT_FOUND;
    }

    return fs_->ReadFile(GetCachePath(key), block->data, &block->size);
}

/*
 * Check cache status:
 *   1. check running status (UP/DOWN)
 *   2. check disk healthy (HEALTHY/UNHEALTHY)
 *   3. check disk free space (full OR not)
 */
BCACHE_ERROR DiskCache::Check(uint8_t want) {
    if (status_.load(std::memory_order_release) != CacheStatus::UP) {
        return BCACHE_ERROR::CACHE_DOWN;
    }

    if ((want & WANT_EXEC) && !IsHealthy()) {
        return BCACHE_ERROR::CACHE_UNHEALTHY;
    } else if ((want & WANT_STAGE) && StageFull()) {
        return BCACHE_ERROR::CACHE_FULL;
    } else if ((want & WANT_CACHE) && CacheFull()) {
        return BCACHE_ERROR::CACHE_FULL;
    }
    return BCACHE_ERROR::OK;
}

bool DiskCache::IsCached(const BlockKey& key) {
    CacheValue value;
    auto rc = manager_->Get(key, &value);
    if (rc == BCACHE_ERROR::OK) {
        return true;
    } else if (loader_->Loading() &&
               fs_->FileExists(GetCachePath(key))) {
        return true;
    }
    return false;
}

inline std::string DiskCache::GetRootDir() const {
    return layout_->GetRootDir();
}

inline std::string DiskCache::GetStagePath(const BlockKey& key) const {
    return layout_->GetStagePath(key);
}

inline std::string DiskCache::GetCachePath(const BlockKey& key) const {
    return layout_->GetCachePath(key);
}

inline bool DiskCache::Loading() const {
    return loader_.Loading();
}

inline bool DiskCache::IsHealthy() const {
    return true;  // TODO: health checker
}

inline bool DiskCache::StageFull() const {
    return manager_.StageFull();
}

inline bool DiskCache::CacheFull() const {
    return manager_.CacheFull();
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
