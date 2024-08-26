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

#include <butil/time.h>

#include <sstream>

#include "curvefs/src/client/bcache/disk_cache_loader.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curvefs::base::filepath::Join;
using ::curvefs::base::filepath::HasSuffix;

using LocalFileSystem::FileInfo;
using DiskCacheManager::CacheValue;

DiskCacheLoader::DiskCacheLoader(std::shared_ptr<LocalFileSystem> fs,
                                 std::shared_ptr<DiskCacheLayout> layout,
                                 std::shared_ptr<DiskCacheManager> manager)
    : thread_(),
      running_(false),
      fs_(fs),
      layout_(layout),
      manager_(manager) {}

void DiskCacheLoader::Start(UploadFunc uploader) {
    if (!running_.exchange(true)) {
        uploader_ = uploader;
        thread_ = std::thread(&DiskCacheLoader::Load, this);
        LOG(INFO) << "Disk cache loading thread start success.";
    }
}

void DiskCacheLoader::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop disk cache loading thread...";
        thread_.join();
        LOG(INFO) << "Disk cache loading thread stopped.";
    }
}

inline bool DiskCacheLoader::Loading() const {
    return running_.load(std::memory_order_release);
}

// NOTE: if load failed, it will takes up some spaces.
void DiskCacheLoader::Load() {
    DoLoad(layout_->GetStageDir(), LoadType::LOAD_STAGE);
    DoLoad(layout_->GetCacheDir(), LoadType::LOAD_CACHE);
    Stop();
}

void DiskCacheLoader::DoLoad(const std::string& root, LoadType type) {
    BlockKey key;
    uint64_t count = 0, size = 0, ninvalid = 0;
    ::butil::Timer timer;

    timer.start();
    auto rc = fs_->Walk(root, [this](const std::string& prefix,
                                     const FileInfo& info) {
        if (!running_) {
            return BCACHE_ERROR::ABORT;
        }

        std::string path = Join({ prefix, info.name });
        if (HasSuffix(name, ".tmp") || !key.ParseFilename(info.name)) {
            ninvalid++;
            auto err = fs_->RemoveFile(path);
            if (err != BCACHE_ERROR::OK) {
                LOG(WARN) << "Remove invalid block failed: " << StrrErr(err);
            }
            return BCACHE_ERROR::OK;
        }

        if (type == LoadType::LOAD_STAGE) {
            uploader_(key, path);
        } else {  // load cache
            manager_->Add(key, CacheValue(info.size, info.atime));
        }

        count++;
        size += info.size;
        return BCACHE_ERROR::OK;
    });
    timer.stop();

    std::ostringstream oss;
    oss << "Load " << StrType(type) << rc
        << ": " << count << " blocks loaded"
        << ", " << ninvalid << " invalid blocks found"
        << ", it costs " << timer.s_elapsed() << " seconds."
    if (rc == BCACHE_ERROR::OK) {
        LOG(INFO) << oss.str();
    } else {
        LOG(ERROR) << oss.str();
    }
}

std::string DiskCacheLoader::StrType(LoadType type) {
    if (type == LoadType::LOAD_STAGE) {
        return "stage";
    } else if (type == LoadType::LOAD_CACHE) {
        return "cache";
    }
    return "unknown";
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
