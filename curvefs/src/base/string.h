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
 * Created Date: 2024-08-25
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_BASE_STRING_STRING_H_
#define CURVEFS_SRC_BASE_STRING_STRING_H_

#include <stdexcept>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"

namespace curvefs {
namespace base {
namespace string {

using ::absl::StrFormat;
using ::absl::StrSplit;

inline bool Str2Int(const std::string& str, uint64_t* num) noexcept {
  try {
    *num = std::stoull(str);
    return true;
  } catch (std::invalid_argument& e) {
    return false;
  } catch (std::out_of_range& e) {
    return false;
  }
}

inline bool Strs2Ints(const std::vector<std::string>& strs,
                      const std::vector<uint64_t*>& nums) {
  if (strs.size() != nums.size()) {
    return false;
  }

  for (int i = 0; i < strs.size(); i++) {
    Str2Int(strs[i], nums[i]);
  }
  return true;
}

}  // namespace string
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_STRING_STRING_H_
