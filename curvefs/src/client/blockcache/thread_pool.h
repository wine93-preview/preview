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
 * Created Date: 2024-08-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_THREAD_POOL_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_THREAD_POOL_H_

#include <functional>
#include <memory>

#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::TaskThreadPool;

class PutThreadPool {
 public:
  using TaskFunc = std::function<void()>;

 public:
  static PutThreadPool& GetInstance() {
    static PutThreadPool instance;
    return instance;
  }

 public:
  virtual ~PutThreadPool() = default;

  void Init(uint32_t concurrency = 10) {
    pool_ = std::make_unique<TaskThreadPool<>>();
    pool_->Start(concurrency);
  }

  void Stop() { pool_->Stop(); }

  void Enqueue(TaskFunc func) { pool_->Enqueue(func); }

 private:
  std::unique_ptr<TaskThreadPool<>> pool_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_THREAD_POOL_H_
