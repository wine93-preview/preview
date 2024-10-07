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
 * Created Date: 2024-09-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_SEGMENTS_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_SEGMENTS_H_

#include <cstddef>
#include <queue>
#include <vector>

namespace curvefs {
namespace client {
namespace blockcache {

template <typename T>
class Segments {
  using Segment = std::vector<T>;

 public:
  explicit Segments(size_t segment_size);

  void Push(T element);

  Segment Pop(bool peek = false);

  size_t Size();

 private:
  size_t size_;
  size_t segment_size_;
  std::queue<Segment> segments_;
};

template <typename T>
Segments<T>::Segments(size_t segment_size)
    : size_(0), segment_size_(segment_size){};

template <typename T>
void Segments<T>::Push(T element) {
  if (segments_.empty() || segments_.back().size() == segment_size_) {
    segments_.emplace(Segment());
  }
  auto& segment = segments_.back();
  segment.push_back(element);
  size_++;
}

template <typename T>
typename Segments<T>::Segment Segments<T>::Pop(bool peek) {
  if (segments_.empty()) {
    return Segment();
  }

  auto segment = segments_.front();
  if (peek) {
    return segment;
  }
  segments_.pop();
  DCHECK(size_ >= segment.size());
  size_ -= segment.size();
  return segment;
}

template <typename T>
size_t Segments<T>::Size() {
  return size_;
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_SEGMENTS_H_
