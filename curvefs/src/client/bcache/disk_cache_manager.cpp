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

#include "curvefs/src/client/bcache/error.h"
#include "curvefs/src/client/bcache/disk_cache_manager.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curve::common::LockGuard;
using ::curvefs::base::time::Now;
using ::curvefs::base::unit::kMiB;

DiskCacheManager::DiskCacheManager(uint64_t capacity,
                                   double freeSpaceRatio,
                                   std::shared_ptr<LocalFileSystem> fs,
                                   std::shared_ptr<DiskCacheLayout> layout)
    : mutex_(),
      usedBytes_(0),
      capacity_(capacity),
      freeSpaceRatio_(freeSpaceRatio),
      stageFull_(false),
      cacheFull_(false),
      thread_(),
      running_(false),
      sleeper_(),
      fs_(fs),
      layout_(layout),
      caches_() {
    mq_ = std::make_shared<MessageQueueType>("disk_cache_manager", 10);
    mq_->Subscribe([&](const std::shared_ptr<CacheItems>& toDel){
        DeleteBlocks(toDel);
    });
}

void DiskCacheManager::Start() {
    if (!running_.exchange(true)) {
        mq_->Start();
        thread_ = std::thread(&DiskCacheManager::CheckFreeSpace, this);
        LOG(INFO) << "Disk cache manager thread start success.";
    }
}

void DiskCacheManager::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop disk cache manager thread...";
        sleeper_.interrupt();
        thread_.join();
        mq_->Stop();
        LOG(INFO) << "Disk cache manager thread stopped.";
    }
}

void DiskCacheManager::Add(const CacheKey& key,
                           const CacheValue& value) {
    LockGuard lk(mutex_);
    auto it = caches_.find(key);
    if (it != caches_.end()) {  // already exists
        usedBytes_ -= it->second.size;
    }
    caches_[key] = value;
    usedBytes_ += value.size;

    if (usedBytes_ >= capacity_) {
        uint64_t goalBytes = capacity_ * 0.95;
        uint64_t goalFiles = caches_.size() * 0.95;
        CleanupFull(goalBytes, goalFiles);
    }
}

BCACHE_ERROR DiskCacheManager::Get(const CacheKey& key,
                                   CacheValue* value) {
    LockGuard lk(mutex_);
    auto it = caches_.find(key);
    if (it != caches_.end()) {  // already exists
        it->second.atime = Now();
        *value = it->second;
        return BCACHE_ERROR::OK;
    }
    return BCACHE_ERROR::NOT_FOUND;
}

inline bool DiskCacheManager::StageFull() const {
    return stageFull_.load(std::memory_order_acquire);
}

inline bool DiskCacheManager::CacheFull() const {
    return cacheFull_.load(std::memory_order_acquire);
}

void DiskCacheManager::CheckFreeSpace() {
    bool running = true;
    LocalFileSystem::StatFS statfs;

    while (running) {
        auto rc = fs_->StatFS(layout_->GetRootDir(), &statfs);
        if (rc != BCACHE_ERROR::OK) {
            LOG(ERROR) << "Check free space failed: " << StrErr(rc);
            running = sleeper_.wait_for(std::chrono::seconds(1));
            continue;
        }

        double br = statfs.freeBytesRatio;
        double fr = statfs.freeFilesRatio;
        bool cacheFull = br < freeSpaceRatio_ || fr <  freeSpaceRatio_;
        bool stageFull = (br < freeSpaceRatio_ / 2) ||
                         (fr < freeSpaceRatio_ / 2);
        cacheFull_.store(cacheFull, std::memory_order_release);
        stageFull_.store(stageFull, std::memory_order_release);
        if (cacheFull) {
            double watermark = 1.0 - freeSpaceRatio_;
            LOG(WARN) << std::fixed << std::setprecision(2)
                      << "Disk usage is so high, dir=" << layout_->GetRootDir()
                      << ", watermark=" << watermark * 100 << "%"
                      << ", bytes usage=" << (1.0 - br) * 100 << "%"
                      << ", files usage=" << (1.0 - fr) * 100 << "%.";

            LockGuard lk(mutex_);
            goalBytes = statfs.totalBytes * watermark;
            goalFiles = statfs.totalFiles * watermark;
            CleanupFull(goalBytes, goalFiles);
        }
        running = sleeper_.wait_for(std::chrono::seconds(1));
    }
}

// protect by mutex
void DiskCacheManager::CleanupFull(uint64_t goalBytes, uint64_t goalFiles) {
    auto toDel = std::make_shared<CacheItems>();
    for (const auto& it : caches_) {  // TODO: evit by lru
        usedBytes_ -= it.second.size;
        toDel->emplace_back(CacheItem(it.first, it.second));
        if (usedBytes_ <= goalBytes && caches_.size() <= goalFiles) {
            break;
        }
    }
    mq_->Publish(toDel);
}

void DiskCacheManager::DeleteBlocks(const CacheItems& toDel) {
    LOG(INOF) << "Start to delete " << toDel.size() << " blocks."

    uint64_t count = 0, bytes = 0;
    ::butil::Timer timer;
    timer.start();
    for (const auto& item : toDel) {
        auto rc = fs_->RemoveFile(GetCachePath(item.key));
        if (rc != BCACHE_ERROR::OK) {
            LOG(ERROR) << "Delete block " << item.key.Filename()
                       << " failed: " << StrErr(rc);
            continue;
        }
        count++;
        bytes += item.size;
    }
    timer.stop();

    LOG(INFO) << count << " blocks deleted"
              << ", free " << (bytes / kMiB) << "MiB"
              << ", cost " << timer.s_elapsed() << " seconds.";
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) {
    return layout_->GetCachePath(key);
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
