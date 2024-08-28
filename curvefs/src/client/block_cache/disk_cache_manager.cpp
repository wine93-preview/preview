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

#include "curvefs/src/client/block_cache/disk_cache_manager.h"

#include <butil/time.h>

#include <iomanip>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/math.h"
#include "curvefs/src/client/block_cache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::LockGuard;
using ::curvefs::base::math::kMiB;
using ::curvefs::base::time::Now;

DiskCacheManager::DiskCacheManager(uint64_t capacity, double freeSpaceRatio,
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
      layout_(layout) {
  cache_ = std::unique_ptr<Cache>(NewLRUCache(capacity));
  mq_ = std::make_unique<MessageQueueType>("disk_cache_manager", 10);
  mq_->Subscribe(
      [&](const std::shared_ptr<CacheItems>& toDel) { DeleteBlocks(toDel); });
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

void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value) {

  /*
  LockGuard lk(mutex_);
  CacheValue* v = nullptr;
  auto handle = cache_->Lookup(key.Filename());
  auto defer = ::absl::Make if (handle != nullptr) {
    v = reinterpret_cast<CacheValue*>(cache_->Value(handle));
    usedBytes_ -= v->size;
  }


  else {
    v = new CacheValue(std::move(value));

    handle = cache_->Insert(key.Filename(),
                            std::unique_ptr<CacheValue>(value),
                            value.size,

  }

  if (usedBytes_ >= capacity_) {
    uint64_t goalBytes = capacity_ * 0.95;
    uint64_t goalFiles = caches_.size() * 0.95;
    CleanupFull(goalBytes, goalFiles);
  }

  */
}

BCACHE_ERROR DiskCacheManager::Get(const CacheKey& key, CacheValue* value) {
  LockGuard lk(mutex_);
  CacheValue* v = nullptr;
  auto handle = cache_->Lookup(key.Filename());
  auto defer =
      ::absl::MakeCleanup([handle, this]() { cache_->Release(handle); });
  if (handle != nullptr) {
    v = reinterpret_cast<CacheValue*>(cache_->Value(handle));
    v->atime = Now();
    return BCACHE_ERROR::OK;
  }
  return BCACHE_ERROR::NOT_FOUND;
}

void DiskCacheManager::CheckFreeSpace() {
  bool running = true;
  uint64_t goalBytes, goalFiles;
  struct LocalFileSystem::StatDisk stat;

  while (running) {
    auto rc = fs_->GetDiskUsage(layout_->GetRootDir(), &stat);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Check free space failed: " << StrErr(rc);
      running = sleeper_.wait_for(std::chrono::seconds(1));
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
    running = sleeper_.wait_for(std::chrono::seconds(1));
  }
}

// protect by mutex
void DiskCacheManager::CleanupFull(uint64_t goalBytes, uint64_t goalFiles) {
  //auto toDel = std::make_shared<CacheItems>();
  //for (const auto& it : caches_) {  // TODO: evit by lru
  //  usedBytes_ -= it.second.size;
  //  toDel->emplace_back(CacheItem(it.first, it.second));
  //  if (usedBytes_ <= goalBytes && caches_.size() <= goalFiles) {
  //    break;
  //  }
  //}
  //mq_->Publish(toDel);
}

void DiskCacheManager::CleanupExpire() {
  cache_->Prune();
}

void DiskCacheManager::DeleteBlocks(const std::shared_ptr<CacheItems>& toDel) {
  LOG(INFO) << "Start to delete " << toDel->size() << " blocks.";

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

  LOG(INFO) << count << " blocks deleted"
            << ", free " << (bytes / kMiB) << "MiB"
            << ", cost " << timer.u_elapsed() / 1e6 << " seconds.";
}

std::string DiskCacheManager::GetCachePath(const CacheKey& key) {
  return layout_->GetCachePath(key);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
