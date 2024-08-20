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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/bcache/block_cache.h"

namespace curvefs {
namespace client {
namespace bcache {

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option)
    : option_(option) {
    s3_ = std::make_unique<S3ClientImpl>();
    store_ = std::make_unique<DiskCacheGroup>(option.diskCacheOptions);
}

BCACHE_ERROR BlockCacheImpl::Init() {
    auto uploader = [&](BlockKey& key, const std::string& filepath) {
        auto callback = [](int rc, const std::string& key) {
            if (0 == rc) {
                store_->RemoveStage(key);  // FIXME: log warn
                return true;
            }
            return false;  // retry
        }
        s3_->SyncPut(key.StoreKey(), filepath, callback);
    };

    return store_->Init(uploader);
}

BCACHE_ERROR BlockCacheImpl::Shutdown() {
    return store_->Shutdown();
}

BCACHE_ERROR BlockCacheImpl::Put(const BlockKey& key, const Block& block) {
    BCACHE_ERROR rc;
    PerfContext ctx;
    PerfLogGuard guard([&]() {
        return StrFormat("put(%d,%s,%s): %s %s",
                         key.ino, key.Filename(), block.size,
                         StrErr(rc), ctx.ToString());
    });

    if (option_.stage) {
        rc = store_->Stage(key, block);
        if (rc == BCACHE_ERROR::OK) {
            return rc;
        } else {
            LOG(WARN) << "Stage block " << key.StoreKey()
                      << " failed: " << StrErr(rc);
        }
    }
    return s3_->Put(key.StoreKey(), block.data, block.size);
}

BCACHE_ERROR BlockCacheImpl::Get(const BlockKey& key, Block* block) {
    BCACHE_ERROR rc;
    PerfContext ctx;
    PerfLogGuard log([&]() {
        return StrFormat("get(%d,%s,%d): %s %s",
                         key.ino, key.Filename(), block->size,
                         StrErr(rc), ctx.ToString());
    });

    auto rc = store_->Load(key, block);
    if (rc == BCACHE_ERROR::OK) {
        return rc;
    }

    // not found or failed
    rc = s3_->Get(key.StoreKey(), block->data, &block->size);
    if (rc == BCACHE_ERROR::OK) {
        auto err = store_->Cache(key, *block);
        if (err != BCACHE_ERROR::OK) {
            LOG(WARN) << "Cache block " << key.StoreKey()
                      << " failed: " << StrErr(err);
        }
    }
    return rc;
}

BCACHE_ERROR BlockCacheImpl::Range(const BlockKey& key,
                                  off_t offset,
                                  size_t size,
                                  char* buffer) {
    BCACHE_ERROR rc;
    PerfContext ctx;
    PerfLogGuard guard([&]() {
        return StrFormat("range(%d,%s,%s,%s): %s %s",
                         key.ino, key.Filename(), offset, size,
                         StrErr(rc), ctx.ToString());
    });

    Block block;
    rc = store_->Load(key, &block);
    if (rc == BCACHE_ERROR::OK) {
        std::memcpy(buffer, block.data + offset, size);  // TODO(Wine93): IOBuf
        return BCACHE_ERROR::OK;
    }
    return s3_->Range(key.StoreKey(), offset, size, buffer);
}

BCACHE_ERROR BlockCacheImpl::Flush(uint64_t fh) {
    BCACHE_ERROR rc;
    PerfLogContext ctx;
    PerfLogGuard log([&]() {
        return StrFormat("flush(%d): %s %s", fh, StrErr(rc), ctx.ToString());
    });

    return BCACHE_ERROR::OK;  // TODO: let it works
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
