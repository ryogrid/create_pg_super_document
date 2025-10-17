# ResOwnerReleaseCachedPlan

## Location
[src/backend/utils/cache/plancache.c:2242-2245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2242-L2245)

## Overview
A ResourceOwner callback function that releases a cached plan when the resource owner is cleaned up, ensuring proper cleanup of plan cache references to prevent memory leaks.

## Definition

```c
static void
ResOwnerReleaseCachedPlan(Datum res)
```
## Detailed Description
 is a static callback function used by PostgreSQL's ResourceOwner system to automatically release cached plans when resources are cleaned up. This function serves as a bridge between the ResourceOwner cleanup mechanism and the plan cache system. When a resource owner is being torn down (typically at transaction end or on error), this callback is invoked for each cached plan reference that was registered with the resource owner. The function extracts the  pointer from the Datum parameter and calls  to properly decrement reference counts and potentially free the cached plan memory.

This callback is part of PostgreSQL's resource management system that ensures proper cleanup of resources even in error conditions. It's registered in the  structure with a release phase of  and priority , ensuring cached plans are released at the appropriate time during resource cleanup.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the CachedPlan that needs to be released. The pointer is extracted using  and cast to .
## Dependencies
- Functions called/Symbols referenced:
  - : The main function that handles cached plan cleanup
  - : The structure type representing a cached plan
  - : Macro to extract pointer from Datum

- Referenced from:
  - : Used as the release callback in the ResourceOwner descriptor for plan cache references

## Notes and Other Information
- This is a static function only used internally within the plancache.c module
- The function is designed to work with PostgreSQL's ResourceOwner system for automatic resource cleanup
- It ensures that cached plans are properly released even if errors occur during query execution
- The function passes NULL as the second parameter to , which is the  parameter, indicating that the resource owner cleanup is already in progress
- This callback mechanism helps prevent memory leaks by ensuring cached plans don't remain referenced indefinitely

## Simplified Source

```c
static void
ResOwnerReleaseCachedPlan(Datum res)
{
    // Extract CachedPlan pointer from Datum and release it
    ReleaseCachedPlan((CachedPlan *) DatumGetPointer(res), NULL);
}
```