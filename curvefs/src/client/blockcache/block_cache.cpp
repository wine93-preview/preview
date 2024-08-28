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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/block_cache.h"

#include <glog/logging.h>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/client/blockcache/disk_cache_group.h"
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/blockcache/none_cache.h"
#include "curvefs/src/client/blockcache/perf_context.h"
#include "curvefs/src/client/blockcache/thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

StageBlockCount::StageBlockCount() : pending_() {}

void StageBlockCount::Add(uint64_t ino, int64_t n) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto it = pending_.find(ino);
  if (it == pending_.end()) {
    it = pending_.emplace(Value()).first;
  }

  it->second.count += n;
  assert(it->second.count >= 0);
  if (it->second.count == 0) {
    auto cond = it->second.cond;
    pending_.erase(it);
    cond->notify_all();
  }
}

void StageBlockCount::Wait(uint64_t ino) {
  std::unique_lock<std::mutex> lk(mutex_);
  for (;;) {
    auto it = pending_.find(ino);
    if (it == pending_.end()) {
      return;
    }
    it->second.cond->wait(lk);
  }
}

bool StageBlockCount::Empty() {
  std::unique_lock<std::mutex> lk(mutex_);
  return pending_.empty();
}

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option)
    : option_(option),
      running_(false),
      staging_(std::make_unique<StageBlockCount>()) {
  if (option.cacheStore == "none") {
    store_ = std::make_unique<NoneCache>();
  } else {
    store_ = std::make_unique<DiskCacheGroup>(option.diskCacheOptions);
  }
  PutThreadPool::GetInstance().Init(10);
}

BCACHE_ERROR BlockCacheImpl::Init() {
  if (!running_.exchange(true)) {
    return store_->Init([this](const BlockKey& key, const Block& block) {
      Upload(key, block);  // TODO(Wine93): add stage upload queue
    });
  }
}

BCACHE_ERROR BlockCacheImpl::Shutdown() {
  if (running_.exchange(false)) {
    PutThreadPool::GetInstance().Stop();
    store_->Shutdown();
    WaitUploadFinish();
    return BCACHE_ERROR::OK;
  }
}

BCACHE_ERROR BlockCacheImpl::Put(const BlockKey& key, const Block& block) {
  BCACHE_ERROR rc;
  PerfContext ctx;
  BlockCacheLogGuard log([&]() {
    return StrFormat("put(%s,%d): %s %s", key.Filename(), block.size,
                     StrErr(rc), ctx.ToString());
  });

  if (option_.stage) {
    rc = store_->Stage(key, block);
    if (rc == BCACHE_ERROR::OK) {
      staging_->Add(key.ino, 1);
      return rc;
    } else if (rc != BCACHE_ERROR::NOT_SUPPORTED) {
      LOG(WARNING) << "Stage block " << key.StoreKey()
                   << " failed: " << StrErr(rc);
    }
  }
  return S3Client().Put(key.StoreKey(), block.data, block.size);
}

BCACHE_ERROR BlockCacheImpl::Get(const BlockKey& key, Block* block) {
  BCACHE_ERROR rc;
  PerfContext ctx;
  BlockCacheLogGuard log([&]() {
    return StrFormat("get(%s,%d): %s %s", key.Filename(), block->size,
                     StrErr(rc), ctx.ToString());
  });

  /*
  std::shared_ptr<BlockReader> reader;
  auto rc = store_->Load(key, reader);
  if (rc == BCACHE_ERROR::OK) {
    return reader->ReadAt(0, );
  }

  // not found or failed
  rc = s3_->Get(key.StoreKey(), block->data, &block->size);
  if (rc == BCACHE_ERROR::OK) {
    auto err = store_->Cache(key, *block);
    if (err != BCACHE_ERROR::OK && err != BCACHE_ERROR::NOT_SUPPORTED) {
      LOG(WARNING) << "Cache block " << key.StoreKey()
                   << " failed: " << StrErr(err);
    }
  }
  */
  return rc;
}

BCACHE_ERROR BlockCacheImpl::Range(const BlockKey& key, off_t offset,
                                   size_t size, char* buffer, bool retrive) {
  BCACHE_ERROR rc;
  PerfContext ctx;
  BlockCacheLogGuard log([&]() {
    return StrFormat("range(%s,%d,%d): %s %s", key.Filename(), offset, size,
                     StrErr(rc), ctx.ToString());
  });

  std::shared_ptr<BlockReader> reader;
  rc = store_->Load(key, reader);
  if (rc == BCACHE_ERROR::OK) {
    auto defer = ::absl::MakeCleanup([reader]() { reader->Close(); });
    rc = reader->ReadAt(offset, size, buffer);
    if (rc == BCACHE_ERROR::OK) {
      return rc;
    }
  }

  if (rc != BCACHE_ERROR::OK && retrive) {
    rc = S3Client().Range(key.StoreKey(), offset, size, buffer);
  }
  return rc;
}

BCACHE_ERROR BlockCacheImpl::Flush(uint64_t ino) {
  BCACHE_ERROR rc;
  PerfContext ctx;
  BlockCacheLogGuard log([&]() {
    return StrFormat("flush(%d): %s %s", ino, StrErr(rc), ctx.ToString());
  });

  staging_->Wait(ino);
  return BCACHE_ERROR::OK;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) {
  return store_->IsCached(key);
}

void BlockCacheImpl::Upload(const BlockKey& key, const Block& block) {
  auto retry = [&](int code) {
    if (code != 0) {
      LOG(ERROR) << "Object " << key.Filename()
                 << " upload failed, retCode=" << code;
      return true;  // retry
    }

    staging_->Add(key.ino, -1);
    auto rc = store_->RemoveStage(key);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Remove stage (" << key.Filename()
                 << ") failed: " << StrErr(rc);
    }
    return false;
  };
  S3Client().AsyncPut(key.StoreKey(), block.data, block.size, retry);
}

void BlockCacheImpl::WaitUploadFinish() {
  while (running_.load(std::memory_order_relaxed) && !staging_->Empty()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
