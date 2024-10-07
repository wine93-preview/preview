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
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/block_cache_uploader.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/error.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"
#include "curvefs/src/client/blockcache/phase_timer.h"
#include "curvefs/src/client/blockcache/segments.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(drop_page_cache);

void PendingQueue::Push(const StageBlock& stage_block) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto from = stage_block.ctx.from;
  auto iter = queues_.find(from);
  if (iter == queues_.end()) {
    iter = queues_.emplace(from, Segments<StageBlock>(kSegmentSize)).first;
  }
  auto& queue = iter->second;
  queue.Push(stage_block);
  count_[from]++;
}

std::vector<StageBlock> PendingQueue::Pop(bool peek) {
  static std::vector<BlockFrom> pop_prority{
      BlockFrom::CTO_FLUSH,
      BlockFrom::NOCTO_FLUSH,
      BlockFrom::RELOAD,
  };

  std::unique_lock<std::mutex> lk(mutex_);
  for (const auto& from : pop_prority) {
    auto iter = queues_.find(from);
    if (iter != queues_.end() && iter->second.Size() != 0) {
      auto stage_blocks = iter->second.Pop(peek);
      if (peek) {
        return stage_blocks;
      }

      CHECK(count_[from] >= stage_blocks.size());
      count_[from] -= stage_blocks.size();
      return stage_blocks;
    }
  }
  return std::vector<StageBlock>();
}

size_t PendingQueue::Size() {
  std::unique_lock<std::mutex> lk(mutex_);
  size_t size = 0;
  for (auto& item : queues_) {
    size += item.second.Size();
  }
  return size;
}

void PendingQueue::Stat(struct StatBlocks* stat) {
  std::unique_lock<std::mutex> lk(mutex_);
  stat->num_from_cto = count_[BlockFrom::CTO_FLUSH];
  stat->num_from_nocto = count_[BlockFrom::NOCTO_FLUSH];
  stat->num_from_reload = count_[BlockFrom::RELOAD];
  stat->num_total =
      stat->num_from_cto + stat->num_from_nocto + stat->num_from_reload;
}

UploadingQueue::UploadingQueue(size_t capacity) : capacity_(capacity) {}

void UploadingQueue::Push(const StageBlock& stage_block) {
  std::unique_lock<std::mutex> lk(mutex_);
  while (queue_.size() == capacity_) {  // full
    not_full_.wait(lk);
  }
  queue_.push(stage_block);
  count_[stage_block.ctx.from]++;
  not_empty_.notify_one();
}

StageBlock UploadingQueue::Pop() {
  std::unique_lock<std::mutex> lk(mutex_);
  while (queue_.empty()) {
    not_empty_.wait(lk);
  }

  CHECK(queue_.size() != 0);
  auto stage_block = queue_.top();
  queue_.pop();
  CHECK(count_[stage_block.ctx.from] > 0);
  count_[stage_block.ctx.from]--;
  not_full_.notify_one();
  return stage_block;
}

size_t UploadingQueue::Size() {
  std::unique_lock<std::mutex> lk(mutex_);
  return queue_.size();
}

void UploadingQueue::Stat(struct StatBlocks* stat) {
  std::unique_lock<std::mutex> lk(mutex_);
  stat->num_from_cto = count_[BlockFrom::CTO_FLUSH];
  stat->num_from_nocto = count_[BlockFrom::NOCTO_FLUSH];
  stat->num_from_reload = count_[BlockFrom::RELOAD];
  stat->num_total =
      stat->num_from_cto + stat->num_from_nocto + stat->num_from_reload;
}

size_t UploadingQueue::Capacity() const { return capacity_; }

BlockCacheUploader::BlockCacheUploader(std::shared_ptr<CacheStore> store,
                                       std::shared_ptr<S3Client> s3,
                                       std::shared_ptr<Countdown> stage_count)
    : store_(store), s3_(s3), stage_count_(stage_count) {
  scan_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("scan_stage_worker");
  upload_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("upload_stage_worker");
}

void BlockCacheUploader::Init(uint64_t upload_workers,
                              uint64_t upload_queue_size) {
  if (!running_.exchange(true)) {
    pending_queue_ = std::make_shared<PendingQueue>();
    uploading_queue_ = std::make_shared<UploadingQueue>(upload_queue_size);

    // scan stage block worker
    CHECK(scan_stage_thread_pool_->Start(1) == 0);
    scan_stage_thread_pool_->Enqueue(&BlockCacheUploader::ScaningWorker, this);

    // upload stage block worker
    CHECK(upload_stage_thread_pool_->Start(upload_workers) == 0);
    for (uint64_t i = 0; i < upload_workers; i++) {
      upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker,
                                         this);
    }
  }
}

void BlockCacheUploader::Shutdown() {
  if (running_.exchange(false)) {
    scan_stage_thread_pool_->Stop();
    upload_stage_thread_pool_->Stop();
  }
}

void BlockCacheUploader::AddStageBlock(const BlockKey& key,
                                       const std::string& stage_path,
                                       BlockContext ctx) {
  static std::atomic<uint64_t> seq_num(0);
  StageBlock stage_block(seq_num.fetch_add(1, std::memory_order_relaxed), key,
                         stage_path, ctx);
  BeforeUpload(stage_block);
  pending_queue_->Push(stage_block);
}

void BlockCacheUploader::ScaningWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_blocks = pending_queue_->Pop(true);

    bool wait = false;
    if (stage_blocks.empty()) {
      wait = true;
    } else if (stage_blocks[0].ctx.from != BlockFrom::CTO_FLUSH &&
               uploading_queue_->Size() >= uploading_queue_->Capacity() * 0.5) {
      // Reserve space for stage blocks which from |CTO_FLUSH|
      wait = true;
    }

    if (wait) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    stage_blocks = pending_queue_->Pop();
    for (const auto& stage_block : stage_blocks) {
      uploading_queue_->Push(stage_block);
    }
  }
}

void BlockCacheUploader::UploadingWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_block = uploading_queue_->Pop();
    UploadStageBlock(stage_block);
  }
}

void BlockCacheUploader::WaitAllStageUploaded() {
  while (pending_queue_->Size() != 0 || uploading_queue_->Size() != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

namespace {
struct DeferVariables {
  DeferVariables(const StageBlock& stage_block)
      : rc(BCACHE_ERROR::OK),
        stage_block(stage_block),
        length(0),
        buffer(nullptr),
        timer(std::make_shared<PhaseTimer>()) {}

  BCACHE_ERROR rc;
  StageBlock stage_block;
  size_t length;
  std::shared_ptr<char> buffer;
  std::shared_ptr<PhaseTimer> timer;
};

};  // namespace

void BlockCacheUploader::UploadStageBlock(const StageBlock& stage_block) {
  auto ctx = std::make_shared<UploadingBlockContext>(stage_block);
  auto log_guard = std::make_shared<LogGuard>([ctx]() {
    return StrFormat("upload_stage(%s,%d): %s%s",
                     ctx->stage_block.key.Filename(), ctx->length,
                     StrErr(ctx->rc), ctx->timer->ToString());
  });

  ctx->timer->NextPhase(Phase::READ_BLOCK);
  ReadBlock(ctx);
  if (ctx->rc == BCACHE_ERROR::OK) {
    ctx->timer->NextPhase(Phase::S3_PUT);
    UploadBlock(ctx);
  } else if (ctx->rc == BCACHE_ERROR::NOT_FOUND) {
    AfterUpload(stage_block, true);
  } else {  // retry
    ctx->timer->NextPhase(Phase::ENQUEUE_UPLOAD);
    uploading_queue_->Push(stage_block);
  }
}

void BlockCacheUploader::ReadBlock(std::shared_ptr<UploadingBlockContext> ctx) {
  auto stage_block = ctx->stage_block;
  auto key = stage_block.key;
  auto stage_path = stage_block.stage_path;

  auto fs = NewTempLocalFileSystem();
  auto rc = fs->ReadFile(stage_path, ctx->buffer, &ctx->length,
                         FLAGS_drop_page_cache);
  if (rc == BCACHE_ERROR::NOT_FOUND) {
    LOG(ERROR) << "Stage block (" << key.Filename()
               << ") already deleted, drop it!";
  } else if (rc != BCACHE_ERROR::OK) {
    LOG(ERROR) << "Read stage block (" << key.Filename()
               << ") failed: " << StrErr(rc);
  }

  ctx->rc = rc;
}

void BlockCacheUploader::UploadBlock(
    std::shared_ptr<UploadingBlockContext> ctx) {
  auto callback = [ctx, this](int code) {
    auto stage_block = ctx->stage_block;
    auto key = stage_block.key;
    if (code != 0) {
      LOG(ERROR) << "Object " << key.Filename()
                 << " upload failed, retCode=" << code;
      return true;  // retry
    }

    AfterUpload(stage_block, false);
    auto rc = store_->RemoveStage(key);
    if (rc != BCACHE_ERROR::OK) {
      LOG(ERROR) << "Remove stage block (" << key.Filename()
                 << ") failed: " << StrErr(rc);
    }
    return false;
  };
  s3_->AsyncPut(ctx->stage_block.key.StoreKey(), ctx->buffer.get(), ctx->length,
                callback);
}

void BlockCacheUploader::BeforeUpload(const StageBlock& stage_block) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, 1, false);
  }
}

void BlockCacheUploader::AfterUpload(const StageBlock& stage_block,
                                     bool has_error) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, -1, has_error);
  }
}

bool BlockCacheUploader::NeedCount(const StageBlock& stage_block) {
  return stage_block.ctx.from == BlockFrom::CTO_FLUSH;
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
