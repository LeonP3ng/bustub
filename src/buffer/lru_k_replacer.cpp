//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  page_head_ = new PNode();
  page_tail_ = new PNode();
  k_ = k;
  replacer_size_ = num_frames;
  page_head_->next_ = page_tail_;
  page_tail_->pre_ = page_head_;
}

LRUKReplacer::~LRUKReplacer() {
  PNode *next;
  PNode *cur = page_head_;
  while (cur != nullptr) {
    next = cur->next_;
    delete cur;
    cur = next;
  }
}

// 返回一个可用的frame
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ <= 0 || evictable_size_ <= 0) {
    return false;
  }
  PNode *deleted = page_tail_->pre_;
  //  LOG_DEBUG("tail is %d,  head is %d, vis is %ld", deleted->value_, page_head_->next_->value_, deleted->visit_);

  //  LOG_DEBUG("%d %d", deleted->value_, deleted->pre_->value_);
  while (deleted != page_head_ && !deleted->is_evictable_) {
    deleted = deleted->pre_;
    //    LOG_DEBUG("now is %d, cnt is %ld", deleted->value_, deleted->visit_);
  }
  if (deleted == page_head_) {
    return false;
  }
  //  LOG_DEBUG("haha find %d", deleted->value_);
  deleted->next_->pre_ = deleted->pre_;
  deleted->pre_->next_ = deleted->next_;
  *frame_id = deleted->value_;
  page_map_.erase(*frame_id);
  curr_size_--;
  evictable_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  const std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, true);
  if (page_map_.find(frame_id) != page_map_.end()) {
    PNode *cur_node = page_map_[frame_id];
    if (cur_node->visit_ < k_) {
      cur_node->visit_++;
    }
    auto tmp = cur_node;
    PNode *pre_node = cur_node->pre_;
    tmp->next_->pre_ = pre_node;
    pre_node->next_ = tmp->next_;
    while (pre_node != page_head_ && tmp->visit_ >= pre_node->visit_) {
      //      LOG_DEBUG("tmp is %d, pre is %d", tmp->value_, pre_node->value_);

      tmp = pre_node;
      pre_node = pre_node->pre_;
    }
    pre_node->next_->pre_ = cur_node;
    cur_node->next_ = pre_node->next_;
    cur_node->pre_ = pre_node;
    pre_node->next_ = cur_node;

  } else {
    if (curr_size_ >= replacer_size_) {
      //      PNode *deleted = page_tail_->pre_;
      //      deleted->pre_->next_ = page_tail_;
      //      page_tail_->pre_ = deleted->pre_;
      //      page_map_.erase(deleted->value_);
      //      delete deleted;
      //      curr_size_--;
      // do nothing
      return;
    }
    //    LOG_DEBUG("create new Page %d", frame_id);
    auto new_node = new PNode(frame_id);
    page_map_[frame_id] = new_node;
    auto pos = page_head_->next_;
    while (pos != page_tail_ && pos->visit_ > 1) {
      pos = pos->next_;
    }
    curr_size_++;
    evictable_size_++;
    if (pos == page_tail_) {
      pos->pre_->next_ = new_node;
      new_node->pre_ = pos->pre_;
      new_node->next_ = page_tail_;
      page_tail_->pre_ = new_node;

      return;
    }
    pos->pre_->next_ = new_node;
    new_node->pre_ = pos->pre_;
    pos->pre_ = new_node;
    new_node->next_ = pos;

    //    LOG_DEBUG("after record, size is %ld", evictable_size_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  const std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, true);
  // If a frame was previously evictable and is to be set to non-evictable,
  // then size should decrement.
  if (page_map_.find(frame_id) != page_map_.end()) {
    PNode *tmp = page_map_[frame_id];
    if (tmp->is_evictable_ && !set_evictable) {
      evictable_size_--;
      tmp->is_evictable_ = set_evictable;
    } else if (!tmp->is_evictable_ && set_evictable) {
      evictable_size_++;
      tmp->is_evictable_ = set_evictable;
    }
  } else {
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  const std::scoped_lock<std::mutex> lock(latch_);
  if (page_map_.find(frame_id) != page_map_.end()) {
    PNode *tmp = page_map_[frame_id];
    if (!tmp->is_evictable_) {
      // todo this should be error
      return;
    }
    tmp->pre_->next_ = tmp->next_;
    tmp->next_->pre_ = tmp->pre_;
    page_map_.erase(frame_id);
    curr_size_--;
    evictable_size_--;
  } else {
    return;
  }
}

auto LRUKReplacer::Size() -> size_t {
  const std::scoped_lock<std::mutex> lock(latch_);
  return evictable_size_;
}

void LRUKReplacer::AddToPageMap(frame_id_t frame_id) {
  PNode *tmp;
  if (page_map_.find(frame_id) != page_map_.end() || curr_size_ >= replacer_size_) {
    // do nothing
    tmp = page_tail_->pre_;
    while (!tmp->is_evictable_) {
    }
    return;
  }

  tmp = new PNode(frame_id);
  curr_size_++;
  page_head_->next_->pre_ = tmp;
  tmp->next_ = page_head_->next_;
  tmp->pre_ = page_head_;
  page_head_->next_ = tmp;
  page_map_[frame_id] = tmp;
}

void LRUKReplacer::PrintAll() {
  auto cur = page_head_->next_;
  LOG_INFO("begin --------");
  LOG_INFO("from head to tail");
  while (cur != page_tail_) {
    LOG_INFO("cur is %d", cur->value_);
    cur = cur->next_;
  }

  cur = page_tail_->pre_;
  LOG_INFO("from tail to head");
  while (cur != page_head_) {
    LOG_INFO("cur is %d", cur->value_);
    cur = cur->pre_;
  }
  LOG_INFO("end ----------");
}

}  // namespace bustub
