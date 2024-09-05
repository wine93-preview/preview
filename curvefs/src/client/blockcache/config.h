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
 * Created Date: 2024-09-02
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_CONFIG_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_CONFIG_H_

DECLARE_int32(disk_check_duration_millsecond);

DECLARE_int32(tick_duration_second);
DECLARE_int32(normal2unstable_io_error_num);
DECLARE_int32(unstable2normal_io_succ_num);
DECLARE_int32(unstable2down_second);

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_CONFIG_H_
