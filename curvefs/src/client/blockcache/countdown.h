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
 * Created Date: 2024-09-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_COUNTDOWN_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_COUNTDOWN_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "curvefs/src/client/blockcache/error.h"

namespace curvefs {
namespace client {
namespace blockcache {

class Countdown {
  struct Counter {
    Counter() : count(0), cond(std::make_shared<std::condition_variable>()) {}

    int64_t count;
    std::shared_ptr<std::condition_variable> cond;
  };

 public:
  Countdown() = default;

  void Add(uint64_t key, int64_t n, bool has_error);

  BCACHE_ERROR Wait(uint64_t key);

  size_t Size();

 private:
  std::mutex mutex_;
  std::unordered_map<uint64_t, Counter> counters_;
  std::unordered_map<uint64_t, bool> has_error_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  //  CURVEFS_SRC_CLIENT_BLOCKCACHE_COUNTDOWN_H_
