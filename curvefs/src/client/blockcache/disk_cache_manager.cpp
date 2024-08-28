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

#include "curvefs/src/client/blockcache/disk_cache_manager.h"

#include <butil/time.h>

#include <iomanip>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/math.h"
#include "curvefs/src/base/time.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::LockGuard;
using ::curvefs::base::math::kMiB;
using ::curvefs::base::time::Now;

DiskCacheManager::DiskCacheManager(uint64_t capacity, double freeSpaceRatio,
                                   uint64_t expireSec,
                                   std::shared_ptr<LocalFileSystem> fs,
                                   std::shared_ptr<DiskCacheLayout> layout)
    : mutex_(),
      usedBytes_(0),
      capacity_(capacity),
      freeSpaceRatio_(freeSpaceRatio),
      expireSec_(expireSec),
      stageFull_(false),
      cacheFull_(false),
      running_(false),
      fs_(fs),
      layout_(layout),
      taskPool_(absl::make_unique<TaskThreadPool<>>()),
      caches_(std::make_unique<LRUCache>()) {
  mq_ = std::make_unique<MessageQueueType>("disk_cache_manager", 10);
  mq_->Subscribe(
      [&](const std::shared_ptr<CacheItems>& toDel) { DeleteBlocks(toDel); });
}

void DiskCacheManager::Start() {
  if (!running_.exchange(true)) {
    mq_->Start();
    taskPool_->Start(2);  // CheckFreeSpace, CleanupExpire
    taskPool_->Enqueue(&DiskCacheManager::CheckFreeSpace, this);
    // taskPool_->Enqueue(&DiskCacheManager::CleanupExpire, this);
    LOG(INFO) << "Disk cache manager thread start success.";
  }
}

void DiskCacheManager::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Stop disk cache manager thread...";
    taskPool_->Stop();
    mq_->Stop();
    LOG(INFO) << "Disk cache manager thread stopped.";
  }
}

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value) {
  LockGuard lk(mutex_);
  caches_->Add(key, value);
  usedBytes_ += value.size;
  if (usedBytes_ >= capacity_) {
    uint64_t goalBytes = capacity_ * 0.95;
    uint64_t goalFiles = caches_->Size() * 0.95;
    CleanupFull(goalBytes, goalFiles);
  }
}

BCACHE_ERROR DiskCacheManager::Get(const CacheKey& key, CacheValue* value) {
  LockGuard lk(mutex_);
  if (caches_->Get(key, value)) {
    return BCACHE_ERROR::OK;
  }
  return BCACHE_ERROR::NOT_FOUND;
}

void DiskCacheManager::CheckFreeSpace() {
  bool running = true;
  uint64_t goalBytes, goalFiles;
  struct LocalFileSystem::StatDisk stat;

  while (running_.load(std::memory_order_relaxed)) {
    auto rc = fs_->GetDiskUsage(layout_->GetRootDir(), &stat);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Check free space failed: " << StrErr(rc);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    double br = stat.freeBytesRatio;
    double fr = stat.freeFilesRatio;
    bool cacheFull = br < freeSpaceRatio_ || fr < freeSpaceRatio_;
    bool stageFull = (br < freeSpaceRatio_ / 2) || (fr < freeSpaceRatio_ / 2);
    cacheFull_.store(cacheFull, std::memory_order_release);
    stageFull_.store(stageFull, std::memory_order_release);
    if (cacheFull) {
      double watermark = 1.0 - freeSpaceRatio_;
      LOG(WARNING) << std::fixed << std::setprecision(2)
                   << "Disk usage is so high, dir=" << layout_->GetRootDir()
                   << ", watermark=" << watermark * 100 << "%"
                   << ", bytes usage=" << (1.0 - br) * 100 << "%"
                   << ", files usage=" << (1.0 - fr) * 100 << "%.";

      LockGuard lk(mutex_);
      goalBytes = stat.totalBytes * watermark;
      goalFiles = stat.totalFiles * watermark;
      CleanupFull(goalBytes, goalFiles);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

// protect by mutex
void DiskCacheManager::CleanupFull(uint64_t goalBytes, uint64_t goalFiles) {
  auto toDel = caches_->Evict([&](const CacheValue& value) {
    if (usedBytes_ <= goalBytes && caches_->Size() <= goalFiles) {
      return EvictStatus::EVICT_DONE;
    }
    usedBytes_ -= value.size;
    return EvictStatus::EVICT_OK;
  });
  mq_->Publish(toDel);
}

void DiskCacheManager::DeleteBlocks(const std::shared_ptr<CacheItems>& toDel) {
  uint64_t count = 0, bytes = 0;
  ::butil::Timer timer;
  timer.start();

  for (const auto& item : *toDel) {
    auto rc = fs_->RemoveFile(GetCachePath(item.key));
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Delete block " << item.key.Filename()
                 << " failed: " << StrErr(rc);
      continue;
    }
    count++;
    bytes += item.value.size;
  }
  timer.stop();

  LOG(INFO) << count << " cache blocks deleted"
            << ", free " << (bytes / kMiB) << "MiB"
            << ", costs " << timer.u_elapsed() / 1e6 << " seconds.";
}

void DiskCacheManager::CleanupExpire() {
  uint64_t n = 0, deleted = 0, freed = 0;
  auto now = Now();
  auto expire = TimeSpec(expireSec_);

  while (running_.load(std::memory_order_relaxed)) {
    LockGuard lk(mutex_);
    auto toDel = caches_->Evict([&](const CacheValue& value) {
      if (++n > 1e3) {
        return EvictStatus::EVICT_DONE;
      } else if (value.atime + expire > now) {
        return EvictStatus::EVICT_SKIP;
      }

      deleted++;
      freed += value.size;
      usedBytes_ -= value.size;
      return EvictStatus::EVICT_OK;
    });

    if (toDel->size() > 0) {
      mq_->Publish(toDel);
      VLOG(3) << deleted << " block expired, free " << (freed / kMiB) << "MiB";
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
