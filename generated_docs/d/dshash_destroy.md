# dshash_destroy

## Location
src/backend/lib/dshash.c: 323 - 366

## Overview
Completely destroys a dynamic shared hash table, freeing all associated memory including entries, buckets, and control structures from the dynamic shared area.

## Definition


## Detailed Description
The dshash_destroy function performs complete cleanup of a shared hash table by iterating through all buckets and freeing every entry in the hash table, then freeing the bucket array and control structure. This is a destructive operation that makes the hash table permanently inaccessible to all backends. The function includes safeguards like magic number validation and intentionally corrupts the control block to help detect programming errors where other backends might attempt to access the destroyed hash table.

The caller must ensure that no other backend will attempt to access the hash table after destruction. Other backends that were previously attached should call dshash_detach to clean up their local resources, but the backend calling dshash_destroy should not call dshash_detach as the local structure is freed as part of the destruction process.

## Parameters / Member Variables
- : Pointer to the dshash_table structure to destroy completely

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validates DSHASH_MAGIC)
  - ensure_valid_bucket_pointers
  - NUM_BUCKETS
  - DsaPointerIsValid
  - dsa_get_address
  - dsa_free
  - pfree
- Called from (representative examples):
  - No direct references found in current codebase (likely called during cleanup or shutdown sequences)

## Notes and Other Information
- Frees all hash table entries by walking through each bucket's linked list
- Corrupts the magic number in the control structure to catch use-after-free errors
- Frees the bucket array, control structure, and backend-local memory
- The caller must not call dshash_detach after calling dshash_destroy
- Other backends must still call dshash_detach to clean up their local memory
- This is a complete teardown operation that cannot be undone