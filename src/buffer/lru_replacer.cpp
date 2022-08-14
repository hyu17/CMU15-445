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
  std::lock_guard<std::mutex> guard(latch_);
  max_page_nums_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * Remove the object that was accessed least recently compared to all the other elements being tracked by the Replacer.
 * @param frame_id stores the object's contents
 * @return true if lru list is not empty, otherwise return false
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  // There is no unused frame.
  if (lru_.empty()) {
    return false;
  }

  // Fetch the Least Recently Used Page, 
  // Buffer Poool Manager will evict it out of memory,
  // we should remove it from LRUReplacer here.
  *frame_id = lru_.back();
  lru_map_.erase(*frame_id);
  lru_.pop_back();
  return true;
}

/**
 * This method should be called after a page is pinned to a frame in the BufferPoolManager.
 * It should remove the frame containing the pinned page from the LRUReplacer.
 * @param frame_id contain the pinned page
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  if (lru_.empty()) {
    return;
  }
  
  // If lru list does not have this frame.
  if (lru_map_.find(frame_id) == lru_map_.end()) {
    return;
  }

  // If lru list has this frame, remove it from LRUReplacer.
  std::list<frame_id_t>::iterator iter = lru_map_[frame_id];
  lru_.erase(iter);
  lru_map_.erase(frame_id);
}

/**
 * This method should be called when the pin_count of a page becomes 0.
 * This method should add the frame containing the unpinned page to the LRUReplacer
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  // If lru list is full, return.
  if (lru_.size() >= max_page_nums_) {
    return;
  }

  // If lru list has this frame, just return.
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;
  }

  lru_.push_front(frame_id);
  lru_map_[frame_id] = lru_.begin();
}

/**
 * This method returns the number of frames that are currently in the LRUReplacer.
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return lru_.size();
}
}  // namespace bustub
