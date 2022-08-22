//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  
  // Fetch Directory Page into memory.
  buffer_pool_manager_->NewPage(&directory_page_id_);
  Page* page = buffer_pool_manager->FetchPage(directory_page_id_);
  dir_page_ = reinterpret_cast<HashTableDirectoryPage*>(page);
  
  // Initialize members of directory.
  dir_page_->page_id_ = directory_page_id_;
  dir_page_->global_depth_ = 1;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t dir_index = KeyToDirectoryIndex(key);
  return dir_page->bucket_page_ids_[dir_index];
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return dir_page_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page* bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(bucket_page);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result)
{
  HashTableDirectoryPage* page_directory = FetchDirectoryPage();

  // Fetch the bucket page into memory.
  uint32_t bucket_page_id = KeyToPageId(key, page_directory);
  HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(bucket_page_id);

  // Search for the value corresponds to the key.
  return bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
// Before using this API, you should make sure if insert will overflow by comparing
// global depth with local depth, if it will not overflow, insert it and return true,
// if global depth is equal to each local depth, split and double the size of directory, 
// otherwise increase the local depth.
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value)
{
  HashTableDirectoryPage* page_directory = FetchDirectoryPage();

  uint32_t dir_index = KeyToDirectoryIndex(key, page_directory);
  // TODO(yh): there is a concurrency problem.
  uint32_t local_depth = page_directory->GetLocalDepth(dir_index);
  uint32_t global_depth = GetGlobalDepth();

  if (local_depth < global_depth)
  {
    // Fetch the bucket page into memory.
    uint32_t bucket_page_id = KeyToPageId(key, page_directory);
    HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(bucket_page_id);
    if (bucket_page->Insert(key, value, comparator_))
    {
      return true;
    }
    else
    {
      // Overflow! In this case, only split bucket page and increase it's local depth.
      // TODO(yh): there is a concurrency problem.
      ++page_directory->local_depths_[dir_index];
      return SplitInsert(transaction, key, value);    
    }
  }
  // Otherwise, split and double the size of directory.
  else if (local_depth == global_depth)
  {
    page_directory->IncrGlobalDepth();
    // TODO(yh): there is a concurrency problem.
    ++page_directory->local_depths_[dir_index];
    return SplitInsert(transaction, key, value);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value)
{
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
