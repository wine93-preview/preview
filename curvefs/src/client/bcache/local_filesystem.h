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
 * Created Date: 2024-08-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_
#define CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_

#include <dirent.h>

#include <string>
#include <functional>

#include "curvefs/src/base/base.h"
#include "curvefs/src/client/bcache/error.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curvefs::base::time::TimeSpec;

// The local filesystem with high-level utilities for block cache
class LocalFileSystem {
 public:
    struct StatFS {
        uint64_t totalBytes;
        uint64_t totalFiles;
        uint64_t freeBytes;
        uint64_t freeFiles;
        double freeBytesRatio;
        double freeFilesRatio;
    };

    struct FileInfo {
        FileInfo(const std::string& name, size_t size, TimeSpec atime)
            : name(name), size(size), atime(atime) {}

        std::string name;
        size_t size;
        TimeSpec atime;
    };

    using WalkFunc = std::function<BCACHE_ERROR(const std::string& prefix,
                                                const FileInfo& info)>;

 public:
    LocalFileSystem() = default;

    ~LocalFileSystem() = default;

    BCACHE_ERROR MkDirs(const std::string& path);

    BCACHE_ERROR Walk(const std::string& prefix, WalkFunc func);

    BCACHE_ERROR WriteFile(const std::string& path,
                           const char* buffer,
                           size_t count);

    BCACHE_ERROR ReadFile(const std::string& path,
                          char* buffer,
                          size_t* count);

    BCACHE_ERROR RemoveFile(const std::string& path);

    BCACHE_ERROR HardLink(const std::string& oldpath,
                          const std::string& newpath);

    BCACHE_ERROR StatFS(const std::string& path,
                        struct StatFS* statfs);

    bool FileExists(const std::string& path);

 private:
    BCACHE_ERROR PosixError(int code, const std::string& context);

    BCACHE_ERROR Stat(const std::string& path, struct stat* stat);

    BCACHE_ERROR MkDir(const std::string& path);

    BCACHE_ERROR OpenDir(const std::string& path, ::DIR** dir);

    BCACHE_ERROR ReadDir(::DIR* dir, struct dirent* dirent);

    BCACHE_ERROR CloseDir(::DIR* dir);

    BCACHE_ERROR Create(const std::string& path, int* fd);

    BCACHE_ERROR Open(const std::string& path, int flags, int* fd);

    BCACHE_ERROR Write(int fd, const char* buffer, size_t count);

    BCACHE_ERROR Read(int fd, char* buffer, size_t count);

    BCACHE_ERROR Close(int fd);

    BCACHE_ERROR Unlink(const std::string& path);

    BCACHE_ERROR Link(const std::string& oldpath, const std::string& newpath);

    BCACHE_ERROR Rename(const std::string& oldpath, const std::string& newpath);

    BCACHE_ERROR StatFS(const std::string& path, struct statfs* statfs);
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_LOCAL_FILESYSTEM_H_
