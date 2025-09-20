# build_attnums_array

## Location
[src/backend/statistics/extended_stats.c:941-985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L941-L985)

## Overview
Transforms a bitmap representation of attributes into an array of AttrNumber values for use in extended statistics processing.

## Definition

```c
AttrNumber *
build_attnums_array(Bitmapset *attrs, int nexprs, int *numattrs)
```
## Detailed Description
This function converts a Bitmapset containing attribute references into a dynamically allocated array of AttrNumber values. It's specifically designed for extended statistics operations where all attributes are user-defined (positive attribute numbers). The function iterates through the bitmap members, adjusts for expression indices, and validates that all resulting attribute numbers are valid user-defined attributes. It includes comprehensive assertions to ensure data integrity and prevent overflows.

## Parameters / Member Variables
- : Bitmapset containing the attributes to be converted to an array
- : Number of expressions that need to be accounted for when calculating attribute numbers
- : Output parameter that receives the count of attributes in the resulting array (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_num_members](bms_num_members.md)
  - [bms_next_member](bms_next_member.md)
  - AttributeNumberIsValid
  - MaxAttrNumber
  - [SortItem](../S/SortItem.md) (type reference)
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- Designed specifically for extended statistics where all attributes are user-defined
- No offset adjustment needed for FirstLowInvalidHeapAttributeNumber since only user-defined attributes are processed
- Includes multiple assertions to validate attribute number ranges and prevent bitmap corruption
- Memory is allocated using palloc() and should be freed by the caller when no longer needed
- The function adjusts bitmap member values by subtracting nexprs to account for expression indices
- Located in src/backend/statistics/extended_stats.c:941-985