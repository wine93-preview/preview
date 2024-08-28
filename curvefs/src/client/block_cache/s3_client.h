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

#ifndef CURVEFS_SRC_CLIENT_BLOCK_CACHE_S3_CLIENT_H_
#define CURVEFS_SRC_CLIENT_BLOCK_CACHE_S3_CLIENT_H_

#include <functional>
#include <ostream>
#include <string>

#include "curvefs/src/client/block_cache/error.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::S3Adapter;
using ::curve::common::S3AdapterOption;

class S3Client {
 public:
  // retry if callback return true
  using AsyncCallback = std::function<bool(int rc, const std::string& key)>;

 public:
  virtual void Init(const S3AdapterOption& option) = 0;

  virtual void Destroy() = 0;

  virtual BCACHE_ERROR Put(const std::string& key, const char* buffer,
                           size_t size) = 0;

  virtual BCACHE_ERROR Get(const std::string& key, char* buffer,
                           size_t* size) = 0;

  virtual BCACHE_ERROR Range(const std::string& key, off_t offset, size_t size,
                             char* buffer) = 0;

  virtual void AsyncPut(const std::string& key, const std::string& filepath,
                        AsyncCallback callback) = 0;
};

class S3ClientImpl : public S3Client {
 public:
  static S3ClientImpl& GetInstance() {
    static S3ClientImpl instance;
    return instance;
  }

 public:
  virtual ~S3ClientImpl() = default;

  void Init(const S3AdapterOption& option) override;

  void Destroy() override;

  BCACHE_ERROR Put(const std::string& key, const char* buffer,
                   size_t size) override;

  BCACHE_ERROR Get(const std::string& key, char* buffer, size_t* size) override;

  BCACHE_ERROR Range(const std::string& key, off_t offset, size_t size,
                     char* buffer) override;

  // TODO: let it more flexible
  void AsyncPut(const std::string& key, const std::string& filepath,
                AsyncCallback callback) override;

 private:
  std::string S3Key(const std::string& key);

 private:
  std::unique_ptr<S3Adapter> client_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCK_CACHE_S3_CLIENT_H_
