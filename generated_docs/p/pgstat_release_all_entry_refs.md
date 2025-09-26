# pgstat_release_all_entry_refs

## Location
src/backend/utils/activity/pgstat_shmem.c: 767 - 778

## Overview
Releases all local references to shared stats entries in the current process to allow proper cleanup during process exit.

## Definition


## Detailed Description
This function is responsible for releasing all local references to shared statistics entries that a process has acquired during its lifetime. It's a critical cleanup function that prevents memory leaks in the shared statistics system. When a process exits, it must release all references to shared stats entries; otherwise, those entries could never be freed from the shared memory segment.

The function works by delegating to  with NULL match criteria, meaning all entries are released. After releasing all references, it verifies that the hash table is empty and destroys the local entry reference hash table structure.

## Parameters / Member Variables
- : Boolean flag indicating whether pending statistics updates should be discarded rather than flushed to shared memory before releasing references

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the actual reference release logic
  - : Destroys the local hash table structure
  - : Global hash table containing local references to shared stats entries

- Called from (representative examples):
  - : Called during process shutdown to clean up statistics references

## Notes and Other Information
- This is a static function only used within the statistics shared memory module
- The function includes safety checks - it returns early if the reference hash table doesn't exist
- After completion,  is set to NULL, indicating no local references remain
- The Assert statement ensures that all references were properly released before destroying the hash table
- This function is essential for preventing shared memory leaks in PostgreSQL's statistics collection system