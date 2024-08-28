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

#include "curvefs/src/client/block_cache/local_filesystem.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/vfs.h>

#include <sstream>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "curvefs/src/base/file.h"
#include "curvefs/src/base/filepath.h"
#include "curvefs/src/base/math.h"
#include "curvefs/src/base/string.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curvefs::base::file::IsDir;
using ::curvefs::base::file::IsFile;
using ::curvefs::base::file::StrMode;
using ::curvefs::base::filepath::Filename;
using ::curvefs::base::filepath::Join;
using ::curvefs::base::filepath::ParentDir;
using ::curvefs::base::math::Divide;
using ::curvefs::base::string::StrFormat;

// posix filesystem
PosixFileSystem::PosixFileSystem()
    : diskState_(std::make_unique<DiskStateMachineImpl>()) {
  diskState_->Start();
}

PosixFileSystem::~PosixFileSystem() { diskState_->Stop(); }

template <typename... Args>
BCACHE_ERROR PosixFileSystem::PosixError(int code, const char* format,
                                         const Args&... args) {
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
    default:  // IO error
      break;
  }

  // log & update disk state
  std::ostringstream message;
  message << StrFormat(format, args...) << ": " << ::strerror(code);
  if (rc == BCACHE_ERROR::IO_ERROR) {
    diskState_->IOErr();
    LOG(ERROR) << message.str();
  } else {
    diskState_->IOSucc();
    if (rc == BCACHE_ERROR::NOT_FOUND) {
      LOG(WARNING) << message.str();
    }
  }
  return rc;
}

BCACHE_ERROR PosixFileSystem::Stat(const std::string& path, struct stat* stat) {
  if (::stat(path.c_str(), stat) < 0) {
    return PosixError(errno, "stat(%s)", path);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::MkDir(const std::string& path, uint16_t mode) {
  if (::mkdir(path.c_str(), mode) != 0) {
    return PosixError(errno, "mkdir(%s,%s)", path, StrMode(mode));
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::OpenDir(const std::string& path, ::DIR** dir) {
  *dir = ::opendir(path.c_str());
  if (nullptr == *dir) {
    return PosixError(errno, "opendir(%s)", path);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::ReadDir(::DIR* dir, struct dirent** dirent) {
  errno = 0;
  *dirent = ::readdir(dir);
  if (nullptr == *dirent) {
    if (errno == 0) {  // no more files
      return BCACHE_ERROR::END_OF_FILE;
    }
    return PosixError(errno, "readdir()");
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::CloseDir(::DIR* dir) {
  ::closedir(dir);
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Create(const std::string& path, int* fd) {
  int flags = O_TRUNC | O_WRONLY | O_CREAT;
  *fd = ::open(path.c_str(), flags, 0644);
  if (*fd < 0) {
    return PosixError(errno, "open(%s,%#x,0644)", path, flags);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Open(const std::string& path, int flags,
                                   int* fd) {
  *fd = ::open(path.c_str(), flags);
  if (*fd < 0) {
    return PosixError(errno, "open(%s,%#x)", path, flags);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::LSeek(int fd, off_t offset, int whence) {
  if (::lseek(fd, offset, whence) < 0) {
    return PosixError(errno, "lseek(%d,%d,%d)", fd, offset, whence);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Write(int fd, const char* buffer, size_t count) {
  while (count > 0) {
    ssize_t nwritten = ::write(fd, buffer, count);
    if (nwritten < 0) {
      if (errno == EINTR) {
        continue;  // retry
      }
      // error
      return PosixError(errno, "write(%d,%d)", fd, count);
    }
    // success
    buffer += nwritten;
    count -= nwritten;
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Read(int fd, char* buffer, size_t count) {
  ssize_t n;
  for (;;) {
    n = ::read(fd, buffer, count);
    if (n < 0) {
      if (errno == EINTR) {
        continue;  // retry
      }
      // error
      return PosixError(errno, "read(%d,%d)", fd, count);
    }
    break;  // success
  }
  return (n != count) ? BCACHE_ERROR::END_OF_FILE : BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Close(int fd) {
  ::close(fd);
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Unlink(const std::string& path) {
  if (::unlink(path.c_str()) < 0) {
    return PosixError(errno, "unlink(%s)", path);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Link(const std::string& oldpath,
                                   const std::string& newpath) {
  if (::link(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "link(%s,%s)", oldpath, newpath);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::Rename(const std::string& oldpath,
                                     const std::string& newpath) {
  if (::rename(oldpath.c_str(), newpath.c_str()) < 0) {
    return PosixError(errno, "rename(%s,%s)", oldpath, newpath);
  }
  return BCACHE_ERROR::OK;
}

BCACHE_ERROR PosixFileSystem::StatFS(const std::string& path,
                                     struct statfs* statfs) {
  if (::statfs(path.c_str(), statfs) < 0) {
    return PosixError(errno, "statfs(%s)", path);
  }
  return BCACHE_ERROR::OK;
}

LocalFileSystem::LocalFileSystem()
    : posix_(std::make_shared<PosixFileSystem>()) {}

BCACHE_ERROR LocalFileSystem::MkDirs(const std::string& path) {
  // The parent diectory already exists in most time
  auto rc = posix_->MkDir(path, 0755);
  if (rc == BCACHE_ERROR::OK) {
    return rc;
  } else if (rc == BCACHE_ERROR::EXIST) {
    struct stat stat;
    rc = posix_->Stat(path, &stat);
    if (rc != BCACHE_ERROR::OK) {
      return rc;
    } else if (!IsDir(&stat)) {
      return BCACHE_ERROR::NOT_DIRECTORY;
    }
    return BCACHE_ERROR::OK;
  } else if (rc == BCACHE_ERROR::NOT_FOUND) {  // parent directory not exist
    rc = MkDirs(ParentDir(path));
    if (rc == BCACHE_ERROR::OK) {
      rc = MkDirs(path);
    }
  }
  return rc;
}

BCACHE_ERROR LocalFileSystem::Walk(const std::string& prefix, WalkFunc func) {
  ::DIR* dir;
  auto rc = posix_->OpenDir(prefix, &dir);
  if (rc != BCACHE_ERROR::OK) {
    return rc;
  }

  struct dirent* dirent;
  struct stat stat;
  auto defer = ::absl::MakeCleanup([dir, this]() { posix_->CloseDir(dir); });
  for (;;) {
    rc = posix_->ReadDir(dir, &dirent);
    if (rc == BCACHE_ERROR::END_OF_FILE) {
      rc = BCACHE_ERROR::OK;
      break;
    } else if (rc != BCACHE_ERROR::OK) {
      break;
    }

    std::string name(dirent->d_name);
    if (name == "." || name == "..") {
      continue;
    }

    std::string path(Join({prefix, name}));
    rc = posix_->Stat(path, &stat);
    if (rc != BCACHE_ERROR::OK) {
      // break
    } else if (IsDir(&stat)) {
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
                                        const char* buffer, size_t count) {
  auto rc = MkDirs(ParentDir(path));
  if (rc != BCACHE_ERROR::OK) {
    return rc;
  }

  int fd;
  std::string tmp = path + ".tmp";
  rc = posix_->Create(tmp, &fd);
  if (rc == BCACHE_ERROR::OK) {
    rc = posix_->Write(fd, buffer, count);
    posix_->Close(fd);
    if (rc == BCACHE_ERROR::OK) {
      rc = posix_->Rename(tmp, path);
    }
  }
  return rc;
}

BCACHE_ERROR LocalFileSystem::ReadFile(const std::string& path, char* buffer,
                                       size_t* count) {
  struct stat stat;
  auto rc = posix_->Stat(path, &stat);
  if (rc == BCACHE_ERROR::OK) {
    return rc;
  } else if (!IsFile(&stat)) {
    return BCACHE_ERROR::NOT_FOUND;
  } else {
    *count = stat.st_size;
  }

  int fd;
  rc = posix_->Open(path, O_RDONLY, &fd);
  if (rc == BCACHE_ERROR::OK) {
    rc = posix_->Read(fd, buffer, *count);
    posix_->Close(fd);
  }
  return rc;
}

BCACHE_ERROR LocalFileSystem::RemoveFile(const std::string& path) {
  return posix_->Unlink(path);
}

BCACHE_ERROR LocalFileSystem::HardLink(const std::string& oldpath,
                                       const std::string& newpath) {
  auto rc = MkDirs(ParentDir(newpath));
  if (rc == BCACHE_ERROR::OK) {
    rc = posix_->Link(oldpath, newpath);
  }
  return rc;
}

bool LocalFileSystem::FileExists(const std::string& path) {
  struct stat stat;
  auto rc = posix_->Stat(path, &stat);
  return rc == BCACHE_ERROR::OK && IsFile(&stat);
}

BCACHE_ERROR LocalFileSystem::GetDiskUsage(const std::string& path,
                                           StatDisk* stat) {
  struct statfs statfs;
  auto rc = posix_->StatFS(path, &statfs);
  if (rc == BCACHE_ERROR::OK) {
    stat->totalBytes = statfs.f_blocks * statfs.f_bsize;
    stat->totalFiles = statfs.f_files;
    stat->freeBytes = statfs.f_bfree * statfs.f_bsize;
    stat->freeFiles = statfs.f_ffree;
    stat->freeBytesRatio = Divide(stat->freeBytes, stat->totalBytes);
    stat->freeFilesRatio = Divide(stat->freeFiles, stat->totalFiles);
  }
  return rc;
}

BCACHE_ERROR LocalFileSystem::Do(DoFunc func) { return func(posix_); }

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
