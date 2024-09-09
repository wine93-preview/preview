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

#ifndef CURVEFS_SRC_BASE_TIMER_TIMER_H_
#define CURVEFS_SRC_BASE_TIMER_TIMER_H_

#include <functional>

namespace curvefs {
namespace base {
namespace timer {

class Timer {
 public:
  Timer() = default;

  virtual ~Timer() = default;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual bool Add(std::function<void()> func, int delay_ms) = 0;

  virtual bool IsStopped() = 0;
};

}  // namespace timer
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_TIMER_TIMER_H_
