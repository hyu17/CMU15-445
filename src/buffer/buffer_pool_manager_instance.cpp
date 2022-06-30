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

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // Please not that we just wanna flush this page, not delete this page!
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  // Reset this page's dirty flag.
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);

  for (auto iter : page_table_) {
    page_id_t page_id = iter.first;
    frame_id_t frame_id = iter.second;

    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    // All the pages in the buffer pool are pinned.
    return nullptr;
  }

  // Update this page's meta-data.
  page_id_t new_page_id = AllocatePage();
  page->page_id_ = new_page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->ResetMemory();
  // Set the page ID output parameter.
  *page_id = new_page_id;
  // Add this page into the page_table.
  page_table_[new_page_id] = frame_id;
  // Remove this page from the LRUReplacer.
  replacer_->Pin(frame_id);

  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];

    Page *requested_page = &pages_[frame_id];
    ++requested_page->pin_count_;
    replacer_->Pin(frame_id);
    return requested_page;
  }

  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    return nullptr;
  }

  // Update this page's meta-data and read in the page content from disk.
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  // Insert this page into the pagetable.
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);

  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  DeallocatePage(page_id);

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *unpin_page = &pages_[frame_id];

  if (unpin_page->GetPinCount() > 0) {
    --unpin_page->pin_count_;
  } else {
    return false;
  }
  if (unpin_page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }

  if (is_dirty) {
    unpin_page->is_dirty_ = true;
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
