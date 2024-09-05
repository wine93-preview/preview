# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("//:copts.bzl", "CURVE_TEST_COPTS")

TEST_BLOCK_CACHE_DEPS = [
    "//curvefs/src/client:fuse_client_lib",
    "@gtest//:gtest",
]

cc_test(
    name = "test_countdown",
    srcs = [ "test_countdown.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_group",
    srcs = [ "test_disk_cache_group.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_layout",
    srcs = [ "test_disk_cache_layout.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_loader",
    srcs = [ "test_disk_cache_loader.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_cache_manager",
    srcs = [ "test_disk_cache_manager.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_disk_state_machine",
    srcs = [ "test_disk_state_machine.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_error",
    srcs = [ "test_error.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_local_filesystem",
    srcs = [ "test_local_filesystem.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_log",
    srcs = [ "test_log.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_lru_cache",
    srcs = [ "test_lru_cache.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)

cc_test(
    name = "test_mem_cache",
    srcs = [ "test_mem_cache.cpp" ],
    deps = TEST_BLOCK_CACHE_DEPS,
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    linkstatic=1,
)
