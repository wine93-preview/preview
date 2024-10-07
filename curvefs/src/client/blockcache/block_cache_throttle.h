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
 * Created Date: 2024-09-26
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_

#include <memory>

#include "curvefs/src/base/timer/timer_impl.h"
#include "src/common/leaky_bucket.h"
#include "src/common/throttle.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::LeakyBucket;
using ::curve::common::Throttle;
using ::curvefs::base::timer::TimerImpl;

enum class StageAdvise {
  STAGE_IT,
  PUT_S3,
};

class BlockCacheThrottleClosure;

class BlockCacheThrottle {
 public:
  BlockCacheThrottle();

  virtual ~BlockCacheThrottle() = default;

  void Start();

  void Stop();

  StageAdvise Add(uint64_t stage_bytes);

  bool IsOverflow();

 private:
  void Reset();

  void UpdateThrottleParam();

 private:
  friend class BlockCacheThrottleClosure;

 private:
  std::mutex mutex_;
  uint64_t current_bandwidth_throttle_mb_;
  bool overflow_;
  std::unique_ptr<LeakyBucket> throttle_;
  std::unique_ptr<TimerImpl> timer_;
};

class BlockCacheThrottleClosure : public ::google::protobuf::Closure {
 public:
  BlockCacheThrottleClosure(BlockCacheThrottle* throttle)
      : throttle_(throttle) {}

  void Run() override {
    throttle_->Reset();
    delete this;
  }

  void Wait() {}

 private:
  BlockCacheThrottle* throttle_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_THROTTLE_H_
