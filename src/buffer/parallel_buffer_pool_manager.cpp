//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  std::lock_guard<std::mutex> guard(latch_);

  num_instances_ = num_instances;
  pool_size_ = pool_size;
  next_instance_ = 0;

  for (size_t i = 0; i < num_instances_; ++i) {
    buffer_pool_managers_.push_back(
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  std::lock_guard<std::mutex> guard(latch_);
  
  for (size_t i = 0; i < num_instances_; ++i) {
    delete buffer_pool_managers_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_managers_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> guard(latch_);

  for (size_t i = 0; i < num_instances_; ++i) {
    BufferPoolManagerInstance *buffer_pool_manager = buffer_pool_managers_[next_instance_];
    Page *new_page = buffer_pool_manager->NewPage(page_id);
    next_instance_ = (next_instance_ + 1) % num_instances_;
    if (new_page != nullptr) {
      return new_page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager = GetBufferPoolManager(page_id);
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; ++i) {
    buffer_pool_managers_[i]->FlushAllPages();
  }
}

}  // namespace bustub
