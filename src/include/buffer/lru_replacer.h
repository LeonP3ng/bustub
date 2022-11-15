//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/logger.h"
#include "unordered_map"
namespace bustub {
struct PNode {
  struct PNode *pre_;
  struct PNode *next_;
  frame_id_t value_;
  PNode() { pre_ = nullptr, next_ = nullptr, value_ = 0; }
  explicit PNode(frame_id_t v) { pre_ = nullptr, next_ = nullptr, value_ = v; }
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  std::mutex lru_latch_;
  size_t capacity_;
  size_t current_size_;
  PNode *head_;
  PNode *tail_;
  std::unordered_map<frame_id_t, PNode *> page_map_;
};

}  // namespace bustub
