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

#ifndef CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LOADER_H_
#define CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LOADER_H_

#include <atomic>
#include <memory>
#include <string>

#include "curvefs/src/client/block_cache/disk_cache_layout.h"
#include "curvefs/src/client/block_cache/disk_cache_manager.h"
#include "curvefs/src/client/block_cache/local_filesystem.h"

namespace curvefs {
namespace client {
namespace blockcache {

class DiskCacheLoader {
  enum LoadType : uint8_t {
    LOAD_STAGE = 1,
    LOAD_CACHE = 2,
  };

 public:
  DiskCacheLoader(std::shared_ptr<LocalFileSystem> fs,
                  std::shared_ptr<DiskCacheLayout> layout,
                  std::shared_ptr<DiskCacheManager> manager);

  void Start(CacheStore::UploadFunc uploader);

  void Stop();

  bool Loading() const;

 private:
  void Load();

  void DoLoad(const std::string& root, LoadType type);

  std::string StrType(LoadType type);

 public:
  CacheStore::UploadFunc uploader_;
  std::thread thread_;
  std::atomic<bool> running_;
  std::shared_ptr<LocalFileSystem> fs_;
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<DiskCacheManager> manager_;
};

inline bool DiskCacheLoader::Loading() const {
  return running_.load(std::memory_order_release);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LOADER_H_
