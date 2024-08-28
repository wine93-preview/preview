/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_
#define CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

#include <cstdint>
#include <string>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/common/common.h"
#include "src/client/config_info.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace client {
namespace common {
using ::curve::common::Configuration;
using ::curve::common::S3AdapterOption;
using ::curve::common::S3InfoOption;
using ::curvefs::client::common::DiskCacheType;
using MdsOption = ::curve::client::MetaServerOption;

struct BlockDeviceClientOptions {
  std::string configPath;
};

struct MetaCacheOpt {
  int metacacheGetLeaderRetry = 3;
  int metacacheRPCRetryIntervalUS = 500;
  int metacacheGetLeaderRPCTimeOutMS = 1000;

  uint16_t getPartitionCountOnce = 3;
  uint16_t createPartitionOnce = 3;
};

struct ExcutorOpt {
  uint32_t maxRetry = 3;
  uint64_t retryIntervalUS = 200;
  uint64_t rpcTimeoutMS = 1000;
  uint64_t rpcStreamIdleTimeoutMS = 500;
  uint64_t maxRPCTimeoutMS = 64000;
  uint64_t maxRetrySleepIntervalUS = 64ull * 1000 * 1000;
  uint64_t minRetryTimesForceTimeoutBackoff = 5;
  uint64_t maxRetryTimesBeforeConsiderSuspend = 20;
  uint32_t batchInodeAttrLimit = 10000;
  bool enableRenameParallel = false;
};

struct LeaseOpt {
  uint32_t refreshTimesPerLease = 5;
  // default = 20s
  uint32_t leaseTimeUs = 20000000;
};

struct SpaceAllocServerOption {
  std::string spaceaddr;
  uint64_t rpcTimeoutMs;
};

struct KVClientManagerOpt {
  int setThreadPooln = 4;
  int getThreadPooln = 4;
};

struct S3ClientAdaptorOption {
  uint64_t blockSize;
  uint64_t chunkSize;
  uint64_t pageSize;
  uint32_t prefetchBlocks;
  uint32_t prefetchExecQueueNum;
  uint32_t intervalSec;
  uint32_t chunkFlushThreads;
  uint32_t flushIntervalSec;
  uint64_t writeCacheMaxByte;
  uint64_t readCacheMaxByte;
  uint32_t readCacheThreads;
  uint32_t nearfullRatio;
  uint32_t baseSleepUs;
  uint32_t maxReadRetryIntervalMs;
  uint32_t readRetryIntervalMs;
  uint32_t objectPrefix;
};

struct S3Option {
  S3ClientAdaptorOption s3ClientAdaptorOpt;
  S3AdapterOption s3AdaptrOpt;
};

struct BlockGroupOption {
  uint32_t allocateOnce;
};

struct BitmapAllocatorOption {
  uint64_t sizePerBit;
  double smallAllocProportion;
};

struct VolumeAllocatorOption {
  std::string type;
  BitmapAllocatorOption bitmapAllocatorOption;
  BlockGroupOption blockGroupOption;
};

struct VolumeOption {
  uint64_t bigFileSize;
  uint64_t volBlockSize;
  uint64_t fsBlockSize;
  VolumeAllocatorOption allocatorOption;
};

struct ExtentManagerOption {
  uint64_t preAllocSize;
};

struct RefreshDataOption {
  uint64_t maxDataSize = 1024;
  uint32_t refreshDataIntervalSec = 30;
};

// { filesystem option
struct KernelCacheOption {
  uint32_t entryTimeoutSec;
  uint32_t dirEntryTimeoutSec;
  uint32_t attrTimeoutSec;
  uint32_t dirAttrTimeoutSec;
};

struct LookupCacheOption {
  uint64_t lruSize;
  uint32_t negativeTimeoutSec;
  uint32_t minUses;
};

struct DirCacheOption {
  uint64_t lruSize;
  uint32_t timeoutSec;
};

struct AttrWatcherOption {
  uint64_t lruSize;
};

struct OpenFilesOption {
  uint64_t lruSize;
  uint32_t deferSyncSecond;
};

struct RPCOption {
  uint32_t listDentryLimit;
};

struct DeferSyncOption {
  uint32_t delay;
  bool deferDirMtime;
};

struct FileSystemOption {
  bool cto;
  std::string noctoSuffix;
  bool disableXAttr;
  uint32_t maxNameLength;
  uint32_t blockSize = 0x10000u;
  KernelCacheOption kernelCacheOption;
  LookupCacheOption lookupCacheOption;
  DirCacheOption dirCacheOption;
  OpenFilesOption openFilesOption;
  AttrWatcherOption attrWatcherOption;
  RPCOption rpcOption;
  DeferSyncOption deferSyncOption;
};
// }

// { block cache option
struct DiskCacheOption {
  uint32_t index;
  std::string cache_dir;
  uint64_t cache_size;
  double free_space_ratio;
  uint64_t cache_expire_seconds;  // seconds
};

struct BlockCacheOption {
  bool stage;
  std::string cache_store;
  uint32_t flush_concurrency;
  uint32_t flush_queue_size;
  uint64_t upload_stage_concurrency;
  uint64_t upload_stage_queue_size;
  std::vector<DiskCacheOption> diskCacheOptions;
};
// }

struct FuseClientOption {
  MdsOption mdsOpt;
  MetaCacheOpt metaCacheOpt;
  ExcutorOpt excutorOpt;
  ExcutorOpt excutorInternalOpt;
  SpaceAllocServerOption spaceOpt;
  BlockDeviceClientOptions bdevOpt;
  S3Option s3Opt;
  ExtentManagerOption extentManagerOpt;
  VolumeOption volumeOpt;
  LeaseOpt leaseOpt;
  RefreshDataOption refreshDataOption;
  KVClientManagerOpt kvClientManagerOpt;
  FileSystemOption fileSystemOption;
  BlockCacheOption blockCacheOption;

  uint32_t listDentryLimit;
  uint32_t listDentryThreads;
  uint32_t dummyServerStartPort;
  bool enableMultiMountPointRename = false;
  bool enableFuseSplice = false;
  uint32_t downloadMaxRetryTimes;
  uint32_t warmupThreadsNum = 10;
};

void InitFuseClientOption(Configuration* conf, FuseClientOption* clientOption);

void SetFuseClientS3Option(FuseClientOption* clientOption,
                           const S3InfoOption& fsS3Opt);

void S3Info2FsS3Option(const curvefs::common::S3Info& s3,
                       S3InfoOption* fsS3Opt);

void InitMdsOption(Configuration* conf, MdsOption* mdsOpt);

void InitLeaseOpt(Configuration* conf, LeaseOpt* leaseOpt);

void RewriteCacheDir(BlockCacheOption* option, std::string uuid);

}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_
