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

#include "curvefs/src/client/blockcache/disk_state_health_checker.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/base/filepath.h"
#include "curvefs/src/base/timer_impl.h"

DEFINE_int32(disk_check_duration_millsecond, 1 * 1000,
             "disk health check duration in millsecond");

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::filepath::Join;

bool DiskStateHealthChecker::Start() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if (running_) {
    return true;
  }

  timer_ = std::make_unique<base::TimerImpl>();
  CHECK(timer_->Start());

  running_ = true;

  timer_->Add([this] { RunCheck(); }, FLAGS_disk_check_duration_millsecond);

  LOG(INFO) << "DiskStateHealthChecker start";
  return true;
}

bool DiskStateHealthChecker::Stop() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  if (!running_) {
    return true;
  }

  LOG(INFO) << "Try to stop DiskStateHealthChecker";

  running_ = false;

  timer_->Stop();

  return true;
}

void DiskStateHealthChecker::RunCheck() {
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    if (!running_) {
      return;
    }
  }

  // The easiest way to probe disk state
  {
    std::unique_ptr<char[]> buffer(new (std::nothrow) char[8192]);
    std::string path = Join({layout_->GetProbeDir(), "probe.tmp"});
    auto defer = ::absl::MakeCleanup([path, this]() { fs_->RemoveFile(path); });
    fs_->WriteFile(path, buffer.get(), sizeof(buffer));
  }

  // The disk maybe broken
  {
    bool find = fs_->FileExists(layout_->GetLockPath());
    meta_file_exist_.store(find, std::memory_order_release);
  }

  timer_->Add([this] { RunCheck(); }, FLAGS_disk_check_duration_millsecond);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
