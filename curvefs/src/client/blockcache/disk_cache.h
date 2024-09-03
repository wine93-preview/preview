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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_

#include <atomic>
#include <memory>
#include <string>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/disk_cache_layout.h"
#include "curvefs/src/client/blockcache/disk_cache_loader.h"
#include "curvefs/src/client/blockcache/disk_cache_manager.h"
#include "curvefs/src/client/blockcache/disk_state_health_checker.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/blockcache/metric.h"
#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::client::common::DiskCacheOption;

class BlockReaderImpl : public BlockReader {
 public:
  BlockReaderImpl(int fd, std::shared_ptr<LocalFileSystem> fs);

  BCACHE_ERROR ReadAt(off_t offset, size_t length, char* buffer) override;

  void Close() override;

 private:
  int fd_;
  std::shared_ptr<LocalFileSystem> fs_;
};

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

  BCACHE_ERROR Load(const BlockKey& key,
                    std::shared_ptr<BlockReader>& reader) override;

  bool IsCached(const BlockKey& key) override;

  std::string Id() override;

 private:
  BCACHE_ERROR LoadId();

  // check running status, disk healthy and disk free space
  BCACHE_ERROR Check(uint8_t want);

  std::string GetRootDir() const;

  std::string GetStagePath(const BlockKey& key) const;

  std::string GetCachePath(const BlockKey& key) const;

  bool IsLoading() const;

  bool IsHealthy() const;

  bool StageFull() const;

  bool CacheFull() const;

 private:
  std::string id_;
  UploadFunc uploader_;
  DiskCacheOption option_;
  std::atomic<CacheStatus> status_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<DiskCacheManager> manager_;
  std::unique_ptr<DiskCacheLoader> loader_;
  std::shared_ptr<DiskCacheMetric> metric_;
  std::unique_ptr<DiskStateHealthChecker> diskChecker_;
};

inline std::string DiskCache::Id() { return id_; }

inline std::string DiskCache::GetRootDir() const {
  return layout_->GetRootDir();
}

inline std::string DiskCache::GetStagePath(const BlockKey& key) const {
  return layout_->GetStagePath(key);
}

inline std::string DiskCache::GetCachePath(const BlockKey& key) const {
  return layout_->GetCachePath(key);
}

inline bool DiskCache::IsLoading() const { return loader_->IsLoading(); }

inline bool DiskCache::IsHealthy() const {
  return diskChecker_->MetaFileExist() && fs_->IsHealthy();
}

inline bool DiskCache::StageFull() const { return manager_->StageFull(); }

inline bool DiskCache::CacheFull() const { return manager_->CacheFull(); }

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_CACHE_H_
