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

  frame_id_t frame_id = page_table_[page_id];
  page_table_.erase(page_id);
  Page* page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->data_);

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);

  for (auto iter : page_table_) {
    page_id_t page_id = iter.first;
    frame_id_t frame_id = iter.second;

    page_table_.erase(page_id);
    Page* page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->data_);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  *page_id = AllocatePage();

  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();

    page_table_[*page_id] = frame_id;
    Page* page = &pages_[frame_id];
    disk_manager_->ReadPage(*page_id, page->data_);
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;

    return page;
  } else {
    // TODO: check
    frame_id_t frame_id = -1;
    if (replacer_->Victim(&frame_id)) {
      Page* replace_page = &pages_[frame_id];
      
      if (replace_page->IsDirty())
        disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());
      
      page_table_.erase(replace_page->GetPageId());
      disk_manager_->ReadPage(*page_id, replace_page->data_);
      replace_page->page_id_ = *page_id;
      replace_page->pin_count_ = 1; 
      replace_page->is_dirty_ = false;

      return replace_page;
    }
  }

  return nullptr;
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
    // P exists, pin it and return it.
    frame_id_t frame_id = page_table_[page_id];
    
    // The cursor of buffer pool is frame_id because all the pages in the buffer pool are called frame.
    Page* request_page = &pages_[frame_id];
    if (!request_page) {
      LOG_INFO("this page should exist!\n");
    } 
    ++request_page->pin_count_;
    
    return request_page;
  } else {
    // If P does not exists, find a replacement page(R) from free list first.
    if (!free_list_.empty()) {
      // if free list is not empty, fetch a lead free page.
      frame_id_t frame_id = free_list_.front();
      free_list_.pop_front();
 
      // add <page_id, frame_id> into pagetable.
      page_table_[page_id] = frame_id;

      Page* request_page = &pages_[frame_id];
      disk_manager_->ReadPage(page_id, request_page->data_);
      request_page->page_id_ = page_id;
      request_page->pin_count_ = 1;
      request_page->is_dirty_ = false;

      return request_page;
    } else {
      // TODO: evict a page, use LRU policy.
      frame_id_t replace_frame_id = -1;
      if (replacer_->Victim(&replace_frame_id)) { // use LRUReplacer::Victim to find a replacement page
        // TODO: check this frame is dirty or not.
        // If it is dirty, flush to disk.
        Page* replace_page = &pages_[replace_frame_id];
        page_id_t replace_page_id = replace_page->GetPageId();
        if (replace_page->IsDirty()) {
          disk_manager_->WritePage(replace_page_id, replace_page->data_);
        }

        // Re-hash pagetable.
        // Delete R from the pagetable and insert P.
        page_table_.erase(replace_page_id);
        page_table_[page_id] = replace_frame_id;
        
        // Update P's metadata and read in the page content from disk. 
        disk_manager_->ReadPage(page_id, replace_page->data_);
        replace_page->page_id_ = page_id;
        replace_page->pin_count_ = 1;
        replace_page->is_dirty_ = false;
        
        return replace_page;
      }
    }
  }
  // If no page is availiable in the free list and all the other pages are currently pinned.
  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);

  DeallocatePage(page_id);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  } else {
    frame_id_t frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    if (!page)
      return true;
    if (page->pin_count_ > 0)
      return false;
    
    page_table_.erase(page_id);
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;

    free_list_.push_back(frame_id);

    return true;
  }
  return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame_id = page_table_[page_id];
  Page* unpin_page = &pages_[frame_id];
  // Pin count is <= 0 before this call, return false.
  if (unpin_page->pin_count_ <= 0)
    return false;

  replacer_->Unpin(frame_id);

  if (is_dirty)
    unpin_page->is_dirty_ = true;
    
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
