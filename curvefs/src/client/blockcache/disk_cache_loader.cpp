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

#include "curvefs/src/client/blockcache/disk_cache_loader.h"

#include <butil/time.h>

#include <sstream>

#include "curvefs/src/base/filepath.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::filepath::HasSuffix;
using ::curvefs::base::filepath::Join;

DiskCacheLoader::DiskCacheLoader(std::shared_ptr<LocalFileSystem> fs,
                                 std::shared_ptr<DiskCacheLayout> layout,
                                 std::shared_ptr<DiskCacheManager> manager)
    : running_(false),
      taskPool_(absl::make_unique<TaskThreadPool<>>()),
      fs_(fs),
      layout_(layout),
      manager_(manager) {}

void DiskCacheLoader::Start(CacheStore::UploadFunc uploader) {
  if (!running_.exchange(true)) {
    uploader_ = uploader;
    taskPool_->Start(2);  // CheckFreeSpace, CleanupExpire
    taskPool_->Enqueue(&DiskCacheLoader::LoadOnce, this, layout_->GetStageDir(),
                       LoadType::LOAD_STAGE);
    taskPool_->Enqueue(&DiskCacheLoader::LoadOnce, this, layout_->GetCacheDir(),
                       LoadType::LOAD_CACHE);
    LOG(INFO) << "Disk cache loading thread start success.";
  }
}

void DiskCacheLoader::Stop() {
  if (running_.exchange(false)) {
    LOG(INFO) << "Stop disk cache loading thread...";
    taskPool_->Stop();
    LOG(INFO) << "Disk cache loading thread stopped.";
  }
}

// NOTE: if load failed, it will takes up some spaces.
void DiskCacheLoader::LoadOnce(const std::string& root, LoadType type) {
  BlockKey key;
  uint64_t count = 0, size = 0, ninvalid = 0;
  ::butil::Timer timer;

  timer.start();
  auto rc = fs_->Walk(root, [&](const std::string& prefix,
                                const LocalFileSystem::FileInfo& info) {
    if (!running_.load(std::memory_order_relaxed)) {
      return BCACHE_ERROR::ABORT;
    }

    std::string path = Join({prefix, info.name});
    if (HasSuffix(info.name, ".tmp") || !key.ParseFilename(info.name)) {
      ninvalid++;
      auto err = fs_->RemoveFile(path);
      if (err != BCACHE_ERROR::OK) {
        LOG(WARNING) << "Remove invalid block failed: " << StrErr(err);
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
  oss << "Load " << StrType(type) << " (" << root << ") " << rc << ": " << count
      << " blocks loaded"
      << ", " << ninvalid << " invalid blocks found"
      << ", costs " << timer.u_elapsed() / 1e6 << " seconds.";
  if (rc == BCACHE_ERROR::OK) {
    LOG(INFO) << oss.str();
  } else {
    LOG(ERROR) << oss.str();
  }
}

std::string DiskCacheLoader::StrType(LoadType type) {
  switch (type) {
    case LoadType::LOAD_STAGE:
      return "stage";
    case LoadType::LOAD_CACHE:
      return "cache";
    default:
      return "unknown";
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
