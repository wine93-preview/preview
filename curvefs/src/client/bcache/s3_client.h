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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_S3_CLIENT_H_
#define CURVEFS_SRC_CLIENT_BCACHE_S3_CLIENT_H_

#include <string>
#include <ostream>
#include <functional>

#include "curvefs/src/client/bcache/error.h"

namespace curvefs {
namespace client {
namespace bcache {

class S3Client {
 public:
    // retry if callback return true
    using SyncCallback = std::function<bool(int rc, const std::string& key)>;

 public:
    virtual BCACHE_ERROR Put(const std::string& key,
                             const char* buffer,
                             size_t size) = 0;

    virtual BCACHE_ERROR Get(const std::string& key,
                             char* buffer,
                             size_t* size) = 0;

    virtual BCACHE_ERROR Range(const std::string& key,
                               off_t offset,
                               size_t size,
                               char* buffer) = 0;

    virtual void SyncPut(const std::string& key,
                         const std::string& filepath,
                         SyncCallback callback) = 0;
};

class S3ClientImpl : public S3Client {
 public:
    S3ClientImpl() = default;

    ~S3ClientImpl() = default;

    BCACHE_ERROR Put(const std::string& key,
                     const char* buffer,
                     size_t size) override;

    BCACHE_ERROR Get(const std::string& key,
                     char* buffer,
                     size_t* size) override;

    BCACHE_ERROR Range(const std::string& key,
                       off_t offset,
                       size_t size,
                       char* buffer) override;

    // TODO: let it more flexible
    void SyncPut(const std::string& key,
                 const std::string& filepath,
                 SyncCallback callback) override;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_S3_CLIENT_H_
