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

#include <cstdarg>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "curvefs/src/client/bcache/local_filesystem.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::absl::StrFormat;
using ::curvefs::base::math::Divide;
using ::curvefs::base::file::IsFile;
using ::curvefs::base::file::IsDir;
using ::curvefs::base::filepath::ParentDir;
using ::curvefs::base::filepath::Filename;
using ::curvefs::base::filepath::Join;
using ::curvefs::base::string::StringPrintV;

BCACHE_ERROR LocalFileSystem::PosixError(int code, const char* format, ...) {
    // code
    auto rc = BCACHE_ERROR::IO_ERROR;
    switch (code) {
    case 0:
        rc = BCACHE_ERROR::OK;
        break;
    case ENOENT:
        rc = BCACHE_ERROR::NOT_FOUND;
        break;
    case EEXIST:
        rc = BCACHE_ERROR::EXIST;
        break;
    default:
        break;
    }

    // context
    va_list ap;
    va_start(ap, format);
    std::string context = StringPrintV(format, ap);
    va_end(ap);

    // message
    std::string message = StrFormat("%s: %s", context, ::strerror(code));
    if (rc == BCACHE_ERROR::NOT_FOUND) {
        LOG(WARN) << message;
    } else if (rc != BCACHE_ERROR::OK) {
        LOG(ERROR) << message;
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::MkDirs(const std::string& path) {
    // The parent diectory already exists in most time
    auto rc = Mkdir(path);
    if (rc == BCACHE_ERROR::OK) {
        return rc;
    } else if (rc == BCACHE_ERROR::EXIST) {
        struct stat stat;
        rc = Stat(path, &stat);
        if (rc != BCACHE_ERROR::OK) {
            return rc;
        } else if (!IsDir(stat)) {
            return BCACHE_ERROR::NOT_DIRECTORY;
        }
        return BCACHE_ERROR::OK;
    } else if (rc == CURVEFS_ERROR::NOT_FOUND) {  // parent directory not exist
        rc = MkDirs(ParentDir(path));
        if (rc == CURVEFS_ERROR::OK) {
            rc = MkDirs(path);
        }
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::Walk(const std::string& prefix, WalkFunc func) {
    ::DIR* dir;
    auto rc = OpenDir(root, &dir);
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    struct dirent dirent;
    struct stat stat;
    auto defer = ::absl::MakeCleanup([dir, this]() { CloseDir(dir); });
    for ( ; ; ) {
        rc = ReadDir(dir, &dirent);
        if (rc == BCACHE_ERROR::END_OF_FILE) {
            break;
        } else if (rc != BCACHE_ERROR::OK) {
            return rc;
        }

        std::string name(dirent.d_name);
        std::string path(Join({ root, name }));
        rc = Stat(path, &stat);
        if (rc != BCACHE_ERROR::OK) {
            return rc;
        } else if (file::IsDir(&stat)) {
            rc = Walk(path, func);
        } else {  // file
            TimeSpec atime(stat.st_atime, 0);
            rc = func(prefix, FileInfo(name, stat.st_size, atime));
        }

        if (rc != BCACHE_ERROR::OK) {
            break;
        }
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::WriteFile(const std::string& path,
                                        const char* buffer,
                                        size_t count) {
    auto rc = MkDirs(ParentDir(path));
    if (rc != BCACHE_ERROR::OK) {
        return rc;
    }

    int fd;
    std::string tmp = path + ".tmp"
    rc = Create(tmp, &fd);
    if (rc == BCACHE_ERROR::OK) {
        rc = Write(fd, buffer, count);
        Close(fd);
        if (rc == BCACHE_ERROR::OK) {
            rc = Rename(tmp, path);
        }
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::ReadFile(const std::string& path,
                                       char* buffer,
                                       size_t* count) {
    struct stat stat;
    auto rc = Stat(path, &stat);
    if (rc == BCACHE_ERROR::OK) {
        return rc;
    } else if (!IsFile(&stat)) {
        return BCACHE_ERROR::NOT_FOUND;
    } else {
        *count = stat.st_size;
    }

    int fd;
    rc = Open(path, O_RDONLY, &fd);
    if (rc == BCACHE_ERROR::OK) {
        rc = Read(fd, buffer, *count);
        Close(fd);
    }
    return rc;
}

BCACHE_ERROR LocalFileSystem::RemoveFile(const std::string& path) {
    return Unlink(path);
}

BCACHE_ERROR LocalFileSystem::HardLink(const std::string& oldpath,
                                       const std::string& newpath) {
    return Link(oldpath, newpath);
}

BCACHE_ERROR LocalFileSystem::StatFS(const std::string& path,
                                     struct StatFS* statfs) {
    struct statfs stat;
    auto rc = StatFS(path, &stat);
    if (rc == BCACHE_ERROR::OK) {
        statfs->totalBytes = stat.f_blocks * stat.f_bsize;
        statfs->totalFiles = stat.f_files;
        statfs->freeBytes = stat.f_bfree * stat.f_bsize;
        statfs->freeFiles = stat.f_ffree;
        statfs->freeBytesRatio = Divide(statfs->freeBytes, statfs->totalBytes);
        statfs->freeFilesRatio = Divide(statfs->freeFiles, statfs->totalFiles);
    }
    return rc;
}

bool LocalFileSystem::FileExists(const std::string& path) {
    struct stat stat;
    auto rc = Stat(path, &stat);
    return rc == BCACHE_ERROR::OK && IsFile(&stat);
}

// internal utilities

BCACHE_ERROR LocalFileSystem::Stat(const std::string& path, struct stat* stat) {
    if (::stat(path, stat) < 0) {
        return PosixError(::errno, "stat(%s)", path);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::MkDir(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0) {
        return PosixError(::errno, "mkdir(%s,0755)", path);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::OpenDir(const std::string& path, ::DIR** dir) {
    *dir = ::opendir(path.c_str());
    if (nullptr == *dir) {
        return PosixError(::errno, "opendir(%s)", path);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::ReadDir(::DIR* dir, struct dirent* dirent) {
    ::errno = 0;
    *dirent = ::readdir(dir);
    if (nullptr == dirent) {
        if (::errno == 0) {  // no more files
            return BCACHE_ERROR::END_OF_FILE;
        }
        return PosixError(::errno, "readdir()");
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::CloseDir(::DIR* dir) {
    ::closedir(dir);
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Create(const std::string& path, int* fd) {
    int flags = O_TRUNC | O_WRONLY | O_CREAT;
    *fd = ::open(path.c_str(), flags, 0644);
    if (*fd < 0) {
        return PosixError(::errno, "open(%s,%#x,0644)", path, flags);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Open(const std::string& path,
                                   int flags,
                                   int* fd) {
    *fd = ::open(path.c_str(), flags);
    if (*fd < 0) {
        return PosixError(::errno, "open(%s,%#x)", path, flags);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Write(int fd, const char* buffer, size_t count) {
    while (count > 0) {
        ssize_t nwritten = ::write(fd, buffer, count);
        if (nwritten < 0) {
            if (::errno == EINTR) {
                continue;  // retry
            }
            // error
            return PosixError(::errno, "write(%d,%d)", fd, count);
        }
        // success
        buffer += nwritten;
        count -= nwrriten;
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Read(int fd, char* buffer, size_t count) {
    for ( ; ; ) {
        int n = ::read(fd, buffer, count);
        if (n < 0) {
            if (::errno == EINTR) {
                continue;  // retry
            }
            // error
            return PosixError(::errno, "read(%d,%d)", fd, count);
        }
        break;  // success
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Close(int fd) {
    ::close(fd);
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Unlink(const std::string& path) {
    if (::unlink(path.c_str()) < 0) {
        return PosixError(::errno, "unlink(%s)", path);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Link(const std::string& oldpath,
                                   const std::string& newpath) {
    if (::link(oldpath.c_str(), newpath.c_str()) < 0) {
        return PosixError(::errno, "link(%s,%s)", oldpath, newpath);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::Rename(const std::string& oldpath,
                                     const std::string& newpath) {
    if (::rename(oldpath.c_str(), newpath.c_str()) < 0) {
        return PosixError(::errno, "rename(%s,%s)", oldpath, newpath);
    }
    return BCACHE_ERROR::OK;
}

BCACHE_ERROR LocalFileSystem::StatFS(const std::string& path,
                                     struct statfs* statfs) {
    if (::statfs(path.c_str(), statfs) < 0) {
        return PosixError(::errno, "statfs(%s)", path);
    }
    return BCACHE_ERROR::OK;
}

}  // namespace bcache
}  // namespace client
}  // namespace curvefs
