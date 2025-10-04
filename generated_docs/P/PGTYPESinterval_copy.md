# PGTYPESinterval_copy

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:1082-1088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L1082-L1088)

## Overview
Copies the contents of one interval structure to another interval structure in the PostgreSQL ECPG pgtypes library.

## Definition
int PGTYPESinterval_copy(interval *intvlsrc, interval *intvldest)

## Detailed Description
This function performs a simple copy operation between two interval structures. It copies both the time component (representing microseconds for time-based parts like days, hours, minutes, seconds) and the month component (representing year and month parts) from the source interval to the destination interval. The function provides a straightforward way to duplicate interval values without needing to allocate new memory, as it assumes both source and destination intervals are already allocated.

The copy operation is a direct field-by-field assignment, making it an efficient way to duplicate interval values. This is particularly useful when working with interval arrays or when needing to preserve interval values across function calls.

## Parameters / Member Variables
- intvlsrc: Pointer to the source interval structure to copy from. This should point to a valid, initialized interval.
- intvldest: Pointer to the destination interval structure to copy to. This should point to an already allocated interval structure.

## Dependencies
- Functions called/Symbols referenced:
  - interval (data type)
- Called from (representative examples):
  - [ecpg_get_data](../e/ecpg_get_data.md) (ECPG data retrieval operations)
  - [main](../m/main.md) (in test programs dt_test.c and dt_test2.c)
  - Client applications needing to duplicate interval values

## Notes and Other Information
- Always returns 0 (success) - this function cannot fail under normal circumstances
- Both source and destination intervals must be pre-allocated before calling this function
- Performs a shallow copy of the interval structure fields
- Does not allocate or free memory - operates on existing interval structures
- Part of the ECPG pgtypes library providing client-side PostgreSQL data type support
- More efficient than converting to string and back when duplicating intervals
- The destination interval will be completely overwritten by the source values

## Simplified Source

```c
int PGTYPESinterval_copy(interval *intvlsrc, interval *intvldest) {
    // Copy time component (microseconds for days, hours, minutes, seconds)
    intvldest->time = intvlsrc->time;

    // Copy month component (years and months)
    intvldest->month = intvlsrc->month;

    return 0; // Always succeeds
}
```