//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  const std::scoped_lock<std::mutex> lock(latch_);
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  const std::scoped_lock<std::mutex> lock(latch_);
  auto idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  const std::scoped_lock<std::mutex> lock(latch_);
  int idx = IndexOf(key);
  std::shared_ptr<Bucket> cur_bucket = dir_[idx];
  return cur_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  const std::scoped_lock<std::mutex> lock(latch_);
  for (auto ii = 0; ii < 5; ii++) {
    auto idx = IndexOf(key);
    LOG_INFO("index of key %lu, global size %d ", idx, GetGlobalDepth());
    // 通过idx找到对应的bucket
    std::shared_ptr<Bucket> cur_bucket = dir_[idx];

    // 按照实验要求，需要先检查，再插入
    // But for this project please detect if the bucket is overflowing
    // and perform the split before an insertion

    // 1. If a key already exists, the value should be updated.
    if (cur_bucket->FindOnlyByKey(key)) {
      LOG_INFO(" already exist");
      cur_bucket->Insert(key, value);
      PrintAllElement();
      return;
    }

    if (!cur_bucket->IsFull()) {
      cur_bucket->Insert(key, value);
      PrintAllElement();
      return;
    }

    // 2. If the bucket is full and can't be inserted, do the following steps before retrying:

    auto mask = 1 << GetLocalDepth(idx);
    LOG_INFO("mask is %d", mask);

    auto global = GetGlobalDepth();
    if (GetLocalDepth(idx) >= global) {
      LOG_DEBUG("bucket depth %d, before global depth %d, total buckets %ld", GetLocalDepth(idx), global_depth_,
                dir_.size());
      // 2.1 increment the global depth and double the size of the directory.
      global_depth_++;
      for (int i = 0; i < 1 << global; i++) {
        dir_.emplace_back(dir_[i]);
      }

      LOG_DEBUG("bucket depth %d, after global depth %d, total buckets %ld", GetLocalDepth(idx), global_depth_,
                dir_.size());
    }

    // 2.2 Increment the local depth of the bucket.
    cur_bucket->IncrementDepth();

    // 创建新的表
    num_buckets_++;
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, GetLocalDepth(idx));
    // split global pointer
    for (auto i = 0; i < 1 << GetGlobalDepth(); i++) {
      if (dir_[i] == cur_bucket && (mask & i)) {
        //        cout << "dir " << i << "point old bucket address" << cur_bucket << endl;
        dir_[i] = new_bucket;
        //        cout << "new bucket address" << new_bucket << endl;
      }
    }

    // 2.3 Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
    // 用从右数第idx位为区分，1是新表
    // split bucket
    std::list<std::pair<K, V>> items = cur_bucket->GetItems();
    for (auto iter = items.begin(); iter != items.end();) {
      if (std::hash<K>()(iter->first) & mask) {
        //        cout << "remove key :" << iter->first << "from " << idx << " , insert " << new_bucket << endl;

        // 插入新的bucket
        new_bucket->Insert(iter->first, iter->second);
        // 从旧的bucket中删除
        cur_bucket->Remove(iter->first);
        iter = items.erase(iter);
      } else {
        iter++;
      }
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::PrintAllElement() {
  //  std::scoped_lock<mutex> lock(latch_);
  //  cout << "*************" << endl;
  for (uint64_t dd = 0; dd < dir_.size(); dd++) {
    //    cout << "bucket " << dd << " depth " << GetLocalDepth(dd) << " print bucket for " << dd << ": " << endl;
    dir_[dd]->PrintElement();
  }
  //  cout << "*************" << endl;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
    iter++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::FindOnlyByKey(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      return true;
    }
    iter++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::PrintElement() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    std::cout << " " << iter->first << " ";
  }
  std::cout << std::endl;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (list_.empty()) {
    return false;
  }
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
    iter++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (IsFull()) {
    LOG_INFO("full return ");
    return false;
  }
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      *iter = std::make_pair(key, value);
      return true;
    }
    iter++;
  }
  list_.push_back(std::make_pair(key, value));
  std::cout << "push " << key << "to list" << std::endl;
  // LOG_DEBUG("push back to list");
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
