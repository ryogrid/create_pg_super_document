# compare_scalars_simple

## Location
[src/backend/statistics/extended_stats.c:919-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L919-L926)

## Overview
A simple comparison function wrapper that compares two Datum values using the provided SortSupport configuration, following the standard qsort interface.

## Definition

```c
int
compare_scalars_simple(const void *a, const void *b, void *arg)
```
## Detailed Description
This function serves as a qsort-compatible comparison wrapper that extracts Datum values from void pointers and delegates the actual comparison to compare_datums_simple. It provides a bridge between the generic qsort interface requirements and PostgreSQL's datum comparison infrastructure. The function is primarily used in contexts where simple scalar values need to be sorted using PostgreSQL's type-specific comparison logic.

## Parameters / Member Variables
- `*a`: Pointer to the first Datum value to compare (cast from const void*)
- `*b`: Pointer to the second Datum value to compare (cast from const void*)
- `*arg`: Pointer to SortSupport structure containing comparison configuration (cast from void*)
## Dependencies
- Functions called/Symbols referenced:
  - [compare_datums_simple](compare_datums_simple.md)
  - [SortSupport](../S/SortSupport.md) (type)
- Called from (representative examples):
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (src/backend/statistics/mcv.c:696, 958)

## Notes and Other Information
- Follows the standard qsort() comparison function interface signature
- Assumes the void pointers actually point to Datum values
- Returns standard comparison result: negative for less-than, zero for equal, positive for greater-than
- Used primarily in MCV (Most Common Values) statistics serialization where scalar values need sorting
- The function provides type safety by delegating to compare_datums_simple rather than implementing comparison logic directly
- Essential for maintaining consistent sorting behavior across PostgreSQL's statistics subsystem

## Simplified Source
```c
int compare_scalars_simple(const void *a, const void *b, void *arg) {
    // Extract Datum values from void pointers and compare
    return compare_datums_simple(*(Datum *) a,
                                *(Datum *) b,
                                (SortSupport) arg);
}
```