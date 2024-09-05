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

#include "curvefs/src/client/blockcache/perf_context.h"

#include <butil/time.h>

#include <unordered_map>

#include "curvefs/src/base/string.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::string::StrFormat;
using ::curvefs::base::string::StrJoin;

static const std::unordered_map<Phase, std::string> phases = {
    {Phase::STAGE_BLOCK, "stage"},
    {Phase::CACHE_BLOCK, "cache"},
    {Phase::LOAD_BLOCK, "load"},
    {Phase::READ_BLOCK, "read"},
    {Phase::S3_PUT, "s3_put"},
    {Phase::S3_GET, "s3_get"},
    {Phase::S3_RANGE, "s3_range"},
    {Phase::OPEN_FILE, "open"},
    {Phase::WRITE_FILE, "write"},
    {Phase::LINK, "link"},
    {Phase::CACHE_ADD, "cache_add"},
    {Phase::ENQUEUE_UPLOAD, "enqueue"},
};

void PhaseTimer::StopLastPhase() {
  if (!traces_.empty()) {
    PhaseTrace& last = traces_.back();
    last.timer.stop();
    last.elapsed = last.timer.u_elapsed() / 1e6;
  }
}

void PhaseTimer::NextPhase(Phase phase) {
  StopLastPhase();
  traces_.emplace_back(PhaseTrace(phase));
}

std::string PhaseTimer::StrPhase(Phase phase) {
  auto it = phases.find(phase);
  if (it != phases.end()) {
    return it->second;
  }
  return "unknown";
}

std::string PhaseTimer::ToString() {
  StopLastPhase();
  std::vector<std::string> out;
  for (const auto& trace : traces_) {
    out.emplace_back(StrFormat("%s:%.6f", StrPhase(trace.phase), trace.elapsed));
  }

  return "(" + StrJoin(out, ",") + ")";
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
