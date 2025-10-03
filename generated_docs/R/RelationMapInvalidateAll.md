# RelationMapInvalidateAll

## Location
[src/backend/utils/cache/relmapper.c:490-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L490-L503)

## Overview
A public function that reloads all currently-valid relation mapping files, used for recovery from shared invalidation (SI) message buffer overflow situations.

## Definition

```c
void
RelationMapInvalidateAll(void)
```
## Detailed Description
The  function serves as a comprehensive cache invalidation mechanism when PostgreSQL cannot be certain about the validity of its relation mapping caches. This situation typically occurs when the shared invalidation message buffer overflows, meaning some invalidation messages may have been lost. To maintain data consistency, the function forces a reload of all currently-valid mapping files (both shared and local). Like , it includes safety checks to ensure that only already-loaded and valid mapping files are reloaded, preventing issues in processes that should not access certain mapping files.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - RELMAPPER_FILEMAGIC (constant)
  - [load_relmap_file](../l/load_relmap_file.md) (function)
  - shared_map (global variable)
  - local_map (global variable)
- Called from (representative examples):
  - [RelationCacheInvalidate](RelationCacheInvalidate.md)
  - MinSizeOfRelmapUpdate (referenced in header)

## Notes and Other Information
- This is a public function, accessible from other parts of PostgreSQL's codebase
- Designed as a "nuclear option" for cache invalidation when the system cannot trust the current state of relation mappings
- Used specifically in SI message buffer overflow scenarios where individual invalidation messages may have been lost
- Checks the magic number for both shared and local maps before attempting to reload, ensuring safety
- More comprehensive than  as it handles both shared and local mappings unconditionally
- Critical for maintaining data consistency in high-traffic scenarios where the invalidation message system becomes overwhelmed
- Part of PostgreSQL's robust cache coherency infrastructure that handles edge cases in distributed cache invalidation

## Simplified Source

```c
void
RelationMapInvalidateAll(void)
{
    // Reload shared relation mapping file if currently valid
    if (shared_map.magic == RELMAPPER_FILEMAGIC)
        load_relmap_file(true, false);

    // Reload local relation mapping file if currently valid
    if (local_map.magic == RELMAPPER_FILEMAGIC)
        load_relmap_file(false, false);
}
```