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
 * Created Date: 2024-09-05
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/countdown.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace curvefs {
namespace client {
namespace blockcache {

class CountdownTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(CountdownTest, Basic) {
  Countdown count;
  ASSERT_TRUE(count.Empty());

  count.Add(1, 10);
  ASSERT_FALSE(count.Empty());

  count.Add(1, -5);
  ASSERT_FALSE(count.Empty());

  count.Add(1, -5);
  ASSERT_TRUE(count.Empty());
}

TEST_F(CountdownTest, Add) {
  Countdown count;
  ASSERT_TRUE(count.Empty());

  count.Add(1, 10);
  count.Add(2, 10);
  ASSERT_FALSE(count.Empty());

  count.Add(1, -10);
  ASSERT_FALSE(count.Empty());
  count.Add(2, -10);
  ASSERT_TRUE(count.Empty());
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
