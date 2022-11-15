//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  //  replacer_ = new LRUReplacer(pool_size);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  // 页表
  delete page_table_;
  // 替换算法
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  LOG_INFO("get lock");
  const std::lock_guard<std::mutex> guard(latch_);
  LOG_INFO("free list length %d", (int)free_list_.size());
  frame_id_t frame_id = -1;
  Page *target_page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    target_page = &pages_[frame_id];
    LOG_INFO("get page from free list");
  } else if (replacer_->Evict(&frame_id)) {
    target_page = &pages_[frame_id];
    if (target_page->IsDirty()) {
      disk_manager_->WritePage(target_page->GetPageId(), target_page->GetData());
    }
    page_table_->Remove(target_page->GetPageId());
  } else {
    return nullptr;
  }

  *page_id = AllocatePage();
  LOG_INFO("got page id %d", *page_id);
  target_page->page_id_ = *page_id;
  target_page->is_dirty_ = false;
  target_page->pin_count_ = 1;
  target_page->ResetMemory();
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);
  return target_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  const std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = -1;
  Page *target_page = nullptr;

  // 1.     Search the page table for the requested page (P).
  if (page_table_->Find(page_id, frame_id)) {
    //  1.1    If P exists, pin it and return it immediately.
    target_page = &pages_[frame_id];
    target_page->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return target_page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (free_list_.empty()) {
    if (replacer_->Evict(&frame_id)) {
      target_page = &pages_[frame_id];
      // 2.     If R is dirty, write it back to the disk.
      if (target_page->IsDirty()) {
        disk_manager_->WritePage(target_page->GetPageId(), target_page->GetData());
      }
      // 3.     Delete R from the page table and insert P.
      page_table_->Remove(target_page->GetPageId());
    } else {
      return nullptr;
    }
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
    target_page = &pages_[frame_id];
  }

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  target_page->page_id_ = page_id;
  target_page->pin_count_ = 1;
  target_page->is_dirty_ = false;
  disk_manager_->ReadPage(target_page->GetPageId(), target_page->GetData());
  page_table_->Insert(page_id, frame_id);
  replacer_->SetEvictable(frame_id, false);
  return target_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  const std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  Page *target_page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    target_page = &pages_[frame_id];
    if (target_page->pin_count_ <= 0) {
      return false;
    }
    target_page->pin_count_--;
    target_page->is_dirty_ = is_dirty;
    replacer_->SetEvictable(frame_id, true);
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  const std::scoped_lock<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id = -1;
  Page *target_page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    target_page = &pages_[frame_id];
    if (target_page->IsDirty()) {
      disk_manager_->WritePage(target_page->GetPageId(), target_page->GetData());
      target_page->is_dirty_ = false;
    }
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  int total_page = 0;
  {
    std::scoped_lock<std::mutex> lock(latch_);
    total_page = next_page_id_;
  }
  for (page_id_t page_idx = 0; page_idx < total_page; page_idx++) {
    FetchPgImp(page_idx);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  const std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  Page *target_page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    target_page = &pages_[frame_id];
    // If the page is pinned and cannot be deleted, return false immediately.
    if (target_page->pin_count_ > 0) {
      return false;
    }
    // delete the page from the page table
    page_table_->Remove(page_id);
    // stop tracking the frame in the replacer
    replacer_->SetEvictable(frame_id, false);
    // add the frame back to the free list
    free_list_.emplace_back(frame_id);
    // reset the page's memory and metadata
    target_page->ResetMemory();
    target_page->pin_count_ = 0;
    target_page->is_dirty_ = false;
    target_page->page_id_ = INVALID_PAGE_ID;
    return true;
  }
  // If page_id is not in the buffer pool, do nothing and return true.
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
