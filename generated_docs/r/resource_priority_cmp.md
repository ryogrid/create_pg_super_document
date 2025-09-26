# resource_priority_cmp

## Location
[src/backend/utils/resowner/resowner.c:261-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L261-L283)

## Overview
The  function is a comparison function used for sorting resource elements by their release phase and priority, ensuring proper cleanup order during resource owner destruction.

## Definition

```c
static int
resource_priority_cmp(const void *a, const void *b)
```
## Detailed Description
This function implements a comparison function compatible with standard C library sorting functions (such as qsort). It defines the ordering criteria for resource elements during cleanup operations, ensuring that resources are released in the correct sequence to avoid dependency issues.

The sorting logic operates on two levels:
1. **Primary criterion**: Release phase - Resources are grouped by their release_phase value, with higher-numbered phases processed first (reverse order)
2. **Secondary criterion**: Release priority - Within the same release phase, resources with higher priority values are processed first (also reverse order)

The reverse ordering is intentional and explicitly noted in the source code comments. This ensures that resources are released in the opposite order from their typical dependency chain - higher priority and later phase resources are cleaned up before their dependencies.

## Parameters / Member Variables
- : Pointer to the first ResourceElem structure to compare
- : Pointer to the second ResourceElem structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - ResourceElem (struct type)
  - pg_cmp_u32 (for comparing unsigned 32-bit integers)
- Called from (representative examples):
  - ResourceOwnerSort (as a comparison function for sorting operations)

## Notes and Other Information
- This is a static function following the standard C comparison function signature required by qsort and similar sorting functions
- Returns negative value if  should come before , positive if  should come before , and zero if they are equivalent
- The reverse ordering ensures proper cleanup dependencies are respected during resource release
- The function accesses the  field of ResourceElem structures to examine release_phase and release_priority values
- Uses PostgreSQL's  utility function for reliable unsigned integer comparison
- Critical for maintaining data integrity during transaction rollback and error recovery scenarios