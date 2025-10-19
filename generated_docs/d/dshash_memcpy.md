# dshash_memcpy

## Location
[src/backend/lib/dshash.c:590-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L590-L598)

## Overview
A utility function that provides a standardized interface for memory copying in the dshash (dynamic shared hash) system by forwarding calls to the standard memcpy function.

## Definition
```c
void dshash_memcpy(void *dest, const void *src, size_t size, void *arg)
```

## Detailed Description
dshash_memcpy serves as a wrapper function around the standard library's memcpy function, providing a consistent interface for memory copying operations within PostgreSQL's dynamic shared hash table implementation. This function allows the dshash system to use memcpy as a copy function while maintaining the expected function signature for hash table operations. The function performs byte-by-byte copying from a source memory region to a destination memory region.

## Parameters / Member Variables
- `dest`: Pointer to the destination memory region where data will be copied
- `src`: Pointer to the source memory region from which data will be copied
- `size`: Number of bytes to copy from source to destination
- `arg`: Additional argument parameter (unused in this implementation but required for interface compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard library function)
- Called from (representative examples):
  - SH_DECLARE (in src/backend/utils/activity/pgstat_shmem.c:67)
  - [shared_record_table_hash](../s/shared_record_table_hash.md) (in src/backend/utils/cache/typcache.c:261, 271)

## Notes and Other Information
This function is part of the dshash utility functions that provide standardized interfaces for common operations like comparison, hashing, and copying. The unused `arg` parameter maintains compatibility with the expected function signature for dshash copy functions, allowing for potential future extensions or use cases where additional context might be needed. The function explicitly casts the return value of memcpy to void using `(void)` to indicate that the return value is intentionally ignored, as the dshash copy function interface expects a void return type rather than the pointer return of standard memcpy.

## Simplified Source

```c
void
dshash_memcpy(void *dest, const void *src, size_t size, void *arg)
{
    // Simple wrapper around standard memcpy (void cast ignores return value)
    (void) memcpy(dest, src, size);
}
```