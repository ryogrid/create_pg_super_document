# has_matching_range

## Location
[src/backend/access/brin/brin_minmax_multi.c:921-1044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L921-L1044)

## Overview
Checks if a new value falls within any of the existing ranges in a BRIN minmax multi index using binary search.

## Definition

```c
static bool
has_matching_range(BrinDesc *bdesc, Oid colloid, Ranges *ranges,
				   Datum newval, AttrNumber attno, Oid typid)
```
## Detailed Description
This function determines whether a given value falls within any of the existing ranges stored in a BRIN minmax multi index. It uses an efficient binary search algorithm to locate potential matching ranges.

The search process follows these steps:
1. **Quick elimination**: First checks if the value is outside the absolute minimum and maximum bounds of all ranges
2. **Binary search**: Performs a binary search on the range array to find a range that might contain the value
3. **Range matching**: For each candidate range, checks if the value falls between the range's minimum and maximum values

The function is optimized for performance by:
- Early termination when the value is clearly outside all ranges
- Using binary search instead of linear search through ranges
- Leveraging PostgreSQL's comparison function infrastructure for type-agnostic comparisons

The ranges are stored as pairs of values in the values array, where each range occupies two consecutive positions (minimum at even index, maximum at odd index).

## Parameters / Member Variables
- `*bdesc`: BRIN descriptor containing index metadata and comparison functions
- `colloid`: Collation OID for text-based comparisons
- `*ranges`: Ranges structure containing the sorted ranges to search
- `newval`: The Datum value to check for containment
- `attno`: Attribute number in the BRIN index
- `typid`: Data type OID for the values
## Dependencies
- Functions called/Symbols referenced:
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md) (to get comparison functions)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (for performing value comparisons)
  - [DatumGetBool](../D/DatumGetBool.md) (to extract boolean results)
  - BTLessStrategyNumber (comparison strategy constant)
  - BTGreaterStrategyNumber (comparison strategy constant)
- Called from (representative examples):
  - [range_contains_value](../r/range_contains_value.md) (used for value containment checks)

## Notes and Other Information
- Returns true if the value falls within any existing range, false otherwise
- Uses binary search for O(log n) time complexity instead of O(n) linear search
- Handles empty ranges by returning false immediately
- The binary search maintains loop invariants for correctness
- Supports any data type that has less-than and greater-than comparison operators
- Critical for BRIN index performance during tuple insertion and query processing
- The function assumes ranges are non-overlapping and sorted by minimum values

## Simplified Source

```c
static bool
has_matching_range(BrinDesc *bdesc, Oid colloid, Ranges *ranges,
                   Datum newval, AttrNumber attno, Oid typid)
{
    if (ranges->nranges == 0)
        return false;

    // Get comparison functions
    FmgrInfo *cmpLessFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
                                                             BTLessStrategyNumber);
    FmgrInfo *cmpGreaterFn = minmax_multi_get_strategy_procinfo(bdesc, attno, typid,
                                                                BTGreaterStrategyNumber);

    // Quick bounds check - is value outside all ranges?
    Datum minvalue = ranges->values[0];
    Datum maxvalue = ranges->values[2 * ranges->nranges - 1];

    Datum compar = FunctionCall2Coll(cmpLessFn, colloid, newval, minvalue);
    if (DatumGetBool(compar))
        return false;  // Less than minimum

    compar = FunctionCall2Coll(cmpGreaterFn, colloid, newval, maxvalue);
    if (DatumGetBool(compar))
        return false;  // Greater than maximum

    // Binary search through ranges
    int start = 0;
    int end = ranges->nranges - 1;

    while (start <= end) {
        int midpoint = (start + end) / 2;

        // Get min/max for current range
        Datum rangeMin = ranges->values[2 * midpoint];
        Datum rangeMax = ranges->values[2 * midpoint + 1];

        // Check if value is less than range minimum
        compar = FunctionCall2Coll(cmpLessFn, colloid, newval, rangeMin);
        if (DatumGetBool(compar)) {
            end = midpoint - 1;  // Search left half
            continue;
        }

        // Check if value is greater than range maximum
        compar = FunctionCall2Coll(cmpGreaterFn, colloid, newval, rangeMax);
        if (DatumGetBool(compar)) {
            start = midpoint + 1;  // Search right half
            continue;
        }

        // Value is within this range
        return true;
    }

    return false;
}
```