//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity_ = num_pages;
  current_size_ = 0;
  head_ = new PNode();
  tail_ = new PNode();
  head_->next_ = tail_;
  tail_->pre_ = head_;
}

LRUReplacer::~LRUReplacer() {
  PNode *next;
  PNode *cur = head_;
  while (cur != nullptr) {
    next = cur->next_;
    delete cur;
    cur = next;
  }
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(lru_latch_);
  if (current_size_ <= 0) {
    return false;
  }
  PNode *deleted = tail_->pre_;
  deleted->pre_->next_ = tail_;
  tail_->pre_ = deleted->pre_;
  *frame_id = deleted->value_;
  page_map_.erase(*frame_id);
  current_size_--;
  return true;
}

// pin是从replacer删除一个元素
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(lru_latch_);
  if (page_map_.count(frame_id) == 0) {
    return;
  }
  PNode *tmp = page_map_[frame_id];
  tmp->pre_->next_ = tmp->next_;
  tmp->next_->pre_ = tmp->pre_;
  page_map_.erase(frame_id);
  current_size_--;
  delete tmp;
}

// unpin意思是给replacer增加一个元素
void LRUReplacer::Unpin(frame_id_t frame_id) {
  LOG_DEBUG("unpin frame %d", frame_id);
  std::scoped_lock<std::mutex> lock(lru_latch_);
  PNode *tmp;
  if (page_map_.find(frame_id) != page_map_.end() || current_size_ >= capacity_) {
    return;
  } else {
    tmp = new PNode(frame_id);
    current_size_++;
  }
  head_->next_->pre_ = tmp;
  tmp->next_ = head_->next_;
  tmp->pre_ = head_;
  head_->next_ = tmp;
  page_map_[frame_id] = tmp;
}

auto LRUReplacer::Size() -> size_t { return current_size_; }

}  // namespace bustub
