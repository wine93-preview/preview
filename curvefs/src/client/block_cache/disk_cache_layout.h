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
 * Created Date: 2024-08-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LAYOUT_H_
#define CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LAYOUT_H_

#include <string>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/block_cache/cache_store.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::absl::StrFormat;

/*
 * disk cache layout:
 *
 *  950c9813-ea26-4726-96fd-383b0cd22b20
 *  ├── stage
 *  |   └── blocks
 *  │       └── 0
 *  |           └── 4
 *  │               ├── 2_21626898_4098_0_0
 *  |               ├── 2_21626898_4098_1_0
 *  |               ├── 2_21626898_4098_2_0
 *  |               ├── 2_21626898_4098_3_0
 *  |               └── 2_21626898_4098_4_0
 *  ├── cache
 *  │   └── blocks
 *  |       └── 0
 *  │           ├── 0
 *  |           |   ├── 2_21626898_1_0_0
 *  |           |   ├── 2_21626898_1_1_0
 *  |           |   ├── 2_21626898_1_1_0
 *  |           |   └── 2_21626898_1_1_0
 *  |           └── 4
 *  |               ├── 2_21626898_4096_0_0
 *  |               └── 2_21626898_4097_0_0
 *  ├── probe
 *  └── .meta
 */
class DiskCacheLayout {
 public:
  explicit DiskCacheLayout(const std::string& rootDir) : rootDir_(rootDir) {}

  std::string GetRootDir() const { return rootDir_; }

  std::string GetStageDir() const { return StrFormat("%s/stage", rootDir_); }

  std::string GetCacheDir() const { return StrFormat("%s/cache", rootDir_); }

  std::string GetProbeDir() const { return StrFormat("%s/probe", rootDir_); }

  std::string GetMetaPath() const { return StrFormat("%s/.meta", rootDir_); }

  std::string GetStagePath(const BlockKey& key) const {
    return StrFormat("%s/%s", GetStageDir(), key.StoreKey());
  }

  std::string GetCachePath(const BlockKey& key) const {
    return StrFormat("%s/%s", GetCacheDir(), key.StoreKey());
  }

 private:
  std::string rootDir_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_CACHE_DISK_CACHE_LAYOUT_H_
