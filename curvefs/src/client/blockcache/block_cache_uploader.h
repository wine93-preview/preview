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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/blockcache/countdown.h"
#include "curvefs/src/client/blockcache/log.h"
#include "curvefs/src/client/blockcache/s3_client.h"
#include "curvefs/src/client/blockcache/segments.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::TaskThreadPool;

class BlockCacheMetric;

struct StageBlock {
  StageBlock(uint64_t seq_num, const BlockKey& key,
             const std::string& stage_path, BlockContext ctx)
      : seq_num(seq_num), key(key), stage_path(stage_path), ctx(ctx) {}

  bool operator<(const StageBlock& other) const {
    static std::unordered_map<BlockFrom, uint8_t> priority{
        {BlockFrom::CTO_FLUSH, 0},
        {BlockFrom::NOCTO_FLUSH, 1},
        {BlockFrom::RELOAD, 2},
    };

    if (ctx.from == other.ctx.from) {
      return seq_num > other.seq_num;
    }
    return priority[ctx.from] > priority[other.ctx.from];
  }

  uint64_t seq_num;
  BlockKey key;
  std::string stage_path;
  BlockContext ctx;
};

struct StatBlocks {
  uint64_t num_total;
  uint64_t num_from_cto;
  uint64_t num_from_nocto;
  uint64_t num_from_reload;
};

class PendingQueue {
 public:
  PendingQueue() = default;

  void Push(const StageBlock& block);

  std::vector<StageBlock> Pop();

  size_t Size();

  void Stat(struct StatBlocks* stat);

 private:
  std::mutex mutex_;
  std::unordered_map<BlockFrom, Segments<StageBlock>> queues_;
  std::unordered_map<BlockFrom, uint64_t> count_;
  static constexpr uint64_t kSegmentSize = 100;
};

class UploadingQueue {
 public:
  explicit UploadingQueue(size_t capacity);

  void Push(const StageBlock& block);

  StageBlock Pop();

  size_t Size();

  size_t Capacity() const;

  void Stat(struct StatBlocks* stat);

 private:
  std::mutex mutex_;
  size_t capacity_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::priority_queue<StageBlock> queue_;
  std::unordered_map<BlockFrom, uint64_t> count_;
};

/*
 * How it works:
 *
 *               (add)                     (scan)
 * [stage block]------> [ pending queue ] -------> [ uploading queue ] -> [s3]
 */
class BlockCacheUploader {
 public:
  BlockCacheUploader(std::shared_ptr<CacheStore> store,
                     std::shared_ptr<S3Client> s3,
                     std::shared_ptr<Countdown> stage_count);

  virtual ~BlockCacheUploader() = default;

  void Init(uint64_t upload_workers, uint64_t upload_queue_size);

  void Shutdown();

  void AddStageBlock(const BlockKey& key, const std::string& stage_path,
                     BlockContext ctx);

  void WaitAllStageUploaded();

 private:
  friend class BlockCacheMetric;

 private:
  void ScaningWorker();

  void UploadingWorker();

  void UploadStageBlock(const StageBlock& stage_block);

  BCACHE_ERROR ReadBlock(const StageBlock& stage_block,
                         std::shared_ptr<char>& buffer, size_t* length);

  void UploadBlock(const StageBlock& stage_block, std::shared_ptr<char> buffer,
                   size_t length, std::shared_ptr<LogGuard> log_guard);

  void BeforeUpload(const StageBlock& stage_block);

  void AfterUpload(const StageBlock& stage_block, bool has_error);

  bool NeedCount(const StageBlock& stage_block);

 private:
  std::mutex mutex_;
  std::atomic<bool> running_;
  std::shared_ptr<CacheStore> store_;
  std::shared_ptr<S3Client> s3_;
  std::shared_ptr<Countdown> stage_count_;
  std::shared_ptr<PendingQueue> pending_queue_;
  std::shared_ptr<UploadingQueue> uploading_queue_;
  std::unique_ptr<TaskThreadPool<>> scan_stage_thread_pool_;
  std::shared_ptr<TaskThreadPool<>> upload_stage_thread_pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_UPLOADER_H_
