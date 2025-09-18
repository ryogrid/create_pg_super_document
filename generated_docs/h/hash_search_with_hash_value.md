# hash_search_with_hash_value

## Location
src/backend/utils/hash/dynahash.c: 969 - 1145

## Overview
Core implementation of hash table operations that performs lookup, insertion, or removal using a pre-computed hash value.

## Definition


## Detailed Description
This function provides the core implementation for all hash table operations in PostgreSQL's dynamic hash table system. It accepts a pre-computed hash value (typically from get_hash_value) and performs the requested operation efficiently. The function handles table expansion during insertion, manages collision chains through linear probing, and provides thread-safe operations for partitioned tables. It supports four primary operations: finding entries, inserting new entries (with two error-handling variants), and removing entries. The implementation includes optimizations for partitioned tables and maintains comprehensive statistics when compiled with HASH_STATISTICS.

## Parameters / Member Variables
- : Pointer to the HTAB structure representing the hash table
- : Pointer to the key data for the operation
- : Pre-computed hash value for the key (should be computed using get_hash_value)
- : The operation type (HASH_FIND, HASH_ENTER, HASH_ENTER_NULL, or HASH_REMOVE)
- : Optional pointer to a boolean that indicates whether an existing entry was found

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md), HASHHDR, HASHBUCKET (hash table structures)
  - HASHACTION (operation enumeration)
  - FREELIST_IDX (macro for freelist indexing)
  - IS_PARTITIONED (partitioning check macro)
  - [has_seq_scans](has_seq_scans.md) (function to check for active sequential scans)
  - [expand_table](../e/expand_table.md) (table expansion function)
  - [hash_initial_lookup](hash_initial_lookup.md) (initial bucket lookup)
  - [get_hash_entry](../g/get_hash_entry.md) (entry allocation function)
  - ELEMENTKEY (macro to access element key)
- Called from (representative examples):
  - [hash_search](hash_search.md) (convenience wrapper)
  - [BufTableLookup](../B/BufTableLookup.md), BufTableInsert, BufTableDelete
  - [LockAcquireExtended](../L/LockAcquireExtended.md), SetupLockInTable
  - Various predicate locking functions

## Notes and Other Information
- Returns a pointer to the element's key portion for found/created entries, or NULL for not found
- For HASH_REMOVE operations, the returned pointer becomes dangling after the call
- HASH_ENTER reports out-of-memory errors; HASH_ENTER_NULL returns NULL instead
- Automatically expands the table when load factor becomes too high (during insertion only)
- Table expansion is disabled for partitioned tables, frozen tables, or tables with active sequential scans
- Uses spinlocks for thread safety in partitioned tables
- Maintains collision statistics when HASH_STATISTICS is defined
- The caller is responsible for filling the data portion of newly created entries
- Critical to avoid throwing errors after successful entry creation to prevent table corruption
- Supports both shared memory and local memory allocation depending on table configuration