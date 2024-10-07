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
 * Created Date: 2024-09-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "curvefs/src/client/blockcache/block_cache_throttle.h"
#include "curvefs/src/client/blockcache/block_cache_uploader.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/common/dynamic_config.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(block_cache_stage_bandwidth_throttle_enable);
USING_FLAG(block_cache_stage_bandwidth_throttle_mb);

using ::curve::common::TaskThreadPool;
using ::curvefs::client::common::BlockCacheOption;

static void PrintPending(std::ostream& os, void* arg) {
  auto* pending_queue = reinterpret_cast<PendingQueue*>(arg);
  struct StatBlocks stat;
  pending_queue->Stat(&stat);
  os << stat.num_total << "," << stat.num_from_cto << "," << stat.num_from_nocto
     << "," << stat.num_from_reload;
}

static void PrintUploading(std::ostream& os, void* arg) {
  auto* uploading_queue = reinterpret_cast<UploadingQueue*>(arg);
  struct StatBlocks stat;
  uploading_queue->Stat(&stat);
  os << stat.num_total << "," << stat.num_from_cto << "," << stat.num_from_nocto
     << "," << stat.num_from_reload;
}

static bool IsThrottleEnable(void*) {
  return FLAGS_block_cache_stage_bandwidth_throttle_enable;
}

static uint64_t GetThrottleLimit(void*) {
  return FLAGS_block_cache_stage_bandwidth_throttle_mb;
}

static bool IsThrottleOverflow(void* arg) {
  auto* throttle = reinterpret_cast<BlockCacheThrottle*>(arg);
  return throttle->IsOverflow();
}

class BlockCacheMetric {
 public:
  struct AuxMember {
    AuxMember(std::shared_ptr<BlockCacheUploader> uploader,
              std::shared_ptr<BlockCacheThrottle> throttle)
        : uploader(uploader), throttle(throttle) {}

    std::shared_ptr<BlockCacheUploader> uploader;
    std::shared_ptr<BlockCacheThrottle> throttle;
  };

 public:
  BlockCacheMetric(BlockCacheOption option, AuxMember aux_member)
      : metric_("dingofs_block_cache", aux_member) {
    metric_.upload_stage_workers.set_value(option.upload_stage_workers);
    metric_.upload_stage_queue_size.set_value(option.upload_stage_queue_size);
  }

  virtual ~BlockCacheMetric() = default;

 private:
  struct Metric {
    Metric(const std::string& prefix, AuxMember aux_member)
        : upload_stage_workers(prefix, "upload_stage_workers", 0),
          upload_stage_queue_size(prefix, "upload_stage_queue_size", 0),
          stage_blocks_on_pending(prefix, "stage_blocks_on_pending",
                                  &PrintPending,
                                  aux_member.uploader->pending_queue_.get()),
          stage_blocks_on_uploading(
              prefix, "stage_blocks_on_uploading", &PrintUploading,
              aux_member.uploader->uploading_queue_.get()),
          stage_bandwidth_throttle_enable(prefix,
                                          "stage_bandwidth_throttle_enable",
                                          &IsThrottleEnable, nullptr),
          stage_bandwidth_throttle_mb(prefix, "stage_bandwidth_throttle_mb",
                                      &GetThrottleLimit, nullptr),
          stage_bandwidth_throttle_overflow(
              prefix, "stage_bandwidth_throttle_overflow", &IsThrottleOverflow,
              aux_member.throttle.get()) {}

    bvar::Status<uint32_t> upload_stage_workers;
    bvar::Status<uint32_t> upload_stage_queue_size;
    bvar::PassiveStatus<std::string> stage_blocks_on_pending;
    bvar::PassiveStatus<std::string> stage_blocks_on_uploading;
    bvar::PassiveStatus<bool> stage_bandwidth_throttle_enable;
    bvar::PassiveStatus<uint64_t> stage_bandwidth_throttle_mb;
    bvar::PassiveStatus<bool> stage_bandwidth_throttle_overflow;
  };

  Metric metric_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_
