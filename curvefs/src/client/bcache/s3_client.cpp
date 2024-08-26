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

#include <ostream>
#include <unordered_map>

#include "curvefs/src/client/bcache/s3_client.h"

namespace curvefs {
namespace client {
namespace bcache {

BCACHE_ERROR S3ClientImpl::Put(const std::string& key,
                               const char* buffer,
                               size_t size) {
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR S3ClientImpl::Get(const std::string& key,
                               char* buffer,
                               size_t* size) {
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR S3ClientImpl::Range(const std::string& key,
                                 off_t offset,
                                 size_t size,
                                 char* buffer) {
    return BCACHE_ERROR::OK;
}

void S3ClientImpl::SyncPut(const std::string& key,
                           const std::string& filepath,
                           SyncCallback callback) {
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
