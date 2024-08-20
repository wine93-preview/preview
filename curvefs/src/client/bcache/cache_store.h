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

#ifndef CURVEFS_SRC_CLIENT_BCACHE_CACHE_STORE_H_
#define CURVEFS_SRC_CLIENT_BCACHE_CACHE_STORE_H_

#include <string>
#include <functional>

#include "absl/strings/str_format.h"
#include "curvefs/src/base/base.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::absl::StrFormat;
using ::curvefs::base::string::Str2Int;
using ::curvefs::base::filepath::Split;

struct BlockKey {
    BlockKey(uint64_t fsId,
             uint64_t ino,
             uint64_t id,
             uint64_t index,
             uint64_t version)
        : fsId(fsId),
          ino(ino),
          id(id),
          index(index),
          version(version) {}

    std::string Filename() {
        return StrFormat("%d_%d_%d_%d_%d",
                         fsId, ino, id, index, version);
    }

    std::string StoreKey() {
        return StrFormat("blocks/%d/%d/%s",
                         id / 1000 / 1000, id / 1000, Filename());
    }

    bool ParseFilename(const std::string& filename) {
        auto strs = Split(filename, "/");
        return Str2Int(strs, { &fsId, &ino, &id, &index, &version });
    }

    uint64_t fsId;     // filesystem id
    uint64_t ino;      // inode id
    uint64_t id;       // chunkid
    uint64_t index;    // block index (offset/chunkSize)
    uint64_t version;  // compaction version
};

struct Block {
    Block(const char* data, size_t size)
        : data(data), size(size) {}

    const char* data;
    size_t size;
};

class CacheStore {
 public:
    using UploadFunc = std::function<void(const BlockKey& key,
                                          const std::string& filepath)>;

 public:
    virtual BCACHE_ERROR Init(UploadFunc uploader) = 0;

    virtual BCACHE_ERROR Shutdown() = 0;

    virtual BCACHE_ERROR Stage(const BlockKey& key, const Block& block) = 0;

    virtual BCACHE_ERROR RemoveStage(const BlockKey& key) = 0;

    virtual BCACHE_ERROR Cache(const BlockKey& key, const Block& block) = 0;

    virtual BCACHE_ERROR Load(const BlockKey& key, Block* block) = 0;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_CACHE_STORE_H_
