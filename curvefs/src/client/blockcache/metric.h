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

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_

#include <bvar/bvar.h>

#include <string>

#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::client::common::DiskCacheOption;

class DiskCacheMetric {
 public:
  DiskCacheMetric(DiskCacheOption option)
      : metric_(StrFormat("block_cache.disk_caches[%d]", option.index)) {
    metric_.dir.set_value(option.cacheDir);
    metric_.capacity.set_value(option.cacheSize);
    metric_.freeSpaceRatio.set_value(option.freeSpaceRatio);
    metric_.status.set_value("DOWN");
  }

  virtual ~DiskCacheMetric() = default;

  void SetStatus(const std::string& value) { metric_.status.set_value(value); }

  void AddStageBlock(int64_t n, int64_t bytes) {
    metric_.stageBlocks << n;
    metric_.stageBytes << bytes;
  }

  void AddCacheHit() { metric_.cacheHits << 1; }

  void AddCacheMiss() { metric_.cacheMisses << 1; }

  void AddCacheBlock(int64_t n, int64_t bytes) {
    metric_.cacheBlocks << n;
    metric_.cacheBytes << bytes;
  }

 private:
  struct Metric {
    Metric(const std::string& prefix)
        : dir(prefix, "dir"),
          used(prefix, "used", 0),
          capacity(prefix, "capacity", 0),
          freeSpaceRatio(prefix, "free_space_ratio", 0),
          status(prefix, "status"),
          stageSkips(prefix, "stage_skips"),  // stage
          stageBlocks(prefix, "stage_blocks"),
          stageBytes(prefix, "stage_bytes"),
          cacheHits(prefix, "cache_hits"),  // cache
          cacheMisses(prefix, "cache_misses"),
          cacheBlocks(prefix, "cache_blocks"),
          cacheBytes(prefix, "cache_bytes") {}

    bvar::Status<std::string> dir;
    bvar::Status<int64_t> used;
    bvar::Status<int64_t> capacity;
    bvar::Status<double> freeSpaceRatio;
    bvar::Status<std::string> status;
    bvar::Adder<int64_t> stageSkips;  // stage
    bvar::Adder<int64_t> stageBlocks;
    bvar::Adder<int64_t> stageBytes;
    bvar::Adder<int64_t> cacheHits;  // cache
    bvar::Adder<int64_t> cacheMisses;
    bvar::Adder<int64_t> cacheBlocks;
    bvar::Adder<int64_t> cacheBytes;
  };

  Metric metric_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_METRIC_H_
