//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  page_vec.reserve(num_pages);
  for (size_t i = 0; i < num_pages; i++) {
    page_vec.emplace_back(PageNode(i));
  }
  max_size = num_pages;
  current_pos = 0;
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  const std::lock_guard<std::mutex> guard(mutex_);
  for (size_t i = 0; i < 2 * max_size; i++) {
    current_pos = (current_pos + i) % max_size;
    if (!page_vec[current_pos].pin_flag && !page_vec[current_pos].ref_flag) {
      *frame_id = page_vec[current_pos].index;
      page_vec[current_pos].pin_flag = true;
      page_vec[current_pos].ref_flag = true;
      current_pos++;
      return true;
    }
    if (!page_vec[current_pos].pin_flag && page_vec[current_pos].ref_flag) {
      page_vec[current_pos].ref_flag = false;
    }
  }
  return false;
}

// 使用page
void ClockReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> guard(mutex_);
  page_vec[frame_id % max_size].pin_flag = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> guard(mutex_);
  page_vec[frame_id % max_size].pin_flag = false;
}

auto ClockReplacer::Size() -> size_t {
  const std::lock_guard<std::mutex> guard(mutex_);
  size_t ans = 0;
  for (size_t i = 0; i < max_size; i++) {
    size_t idx = (current_pos + i) % max_size;
    if (!page_vec[idx].ref_flag && !page_vec[idx].pin_flag) {
      ans++;
    }
  }
  return ans;
}

}  // namespace bustub
