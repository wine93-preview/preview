// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_

#include <functional>
#include <memory>
#include <shared_mutex>

#include "curvefs/src/base/timer_impl.h"
#include "curvefs/src/client/blockcache/disk_cache_layout.h"
#include "curvefs/src/client/blockcache/local_filesystem.h"

DECLARE_int32(disk_check_duration_millsecond);

namespace curvefs {
namespace client {
namespace blockcache {

class DiskStateHealthChecker {
 public:
  DiskStateHealthChecker(std::shared_ptr<DiskCacheLayout> layout,
                         std::shared_ptr<LocalFileSystem> fs)
      : layout_(layout), fs_(fs) {}

  virtual ~DiskStateHealthChecker() = default;

  virtual bool Start();

  virtual bool Stop();

  virtual bool MetaFileExist();

 private:
  void RunCheck();

  std::shared_mutex rw_lock_;
  bool running_{false};

  std::atomic<bool> meta_file_exist_{true};
  std::shared_ptr<DiskCacheLayout> layout_;
  std::shared_ptr<LocalFileSystem> fs_;

  std::unique_ptr<base::TimerImpl> timer_;
};

inline bool DiskStateHealthChecker::MetaFileExist() {
  return meta_file_exist_.load(std::memory_order_relaxed);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_DISK_STATE_HEALTH_CHECKER_H_
