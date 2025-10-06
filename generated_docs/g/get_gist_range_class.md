# get_gist_range_class

## Location
[src/backend/utils/adt/rangetypes_gist.c:1704-1730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1704-L1730)

## Overview
Determines the class number for a range type to categorize its properties for GiST indexing operations.

## Definition

```c
static int
get_gist_range_class(RangeType *range)
```
## Detailed Description
This function analyzes a PostgreSQL range type and returns a numeric class identifier that represents a valid combination of the range's properties. The class number is used internally by the GiST (Generalized Search Tree) indexing system to efficiently organize and search range data.

The function examines the range flags and constructs a class number by setting appropriate bits based on the range's characteristics:
- Empty ranges get a special class (CLS_EMPTY)
- Non-empty ranges get a combination of flags for infinite bounds and containment properties
- The maximum possible class number is 8, since CLS_EMPTY cannot be combined with other properties

## Parameters / Member Variables
- `*range`: A pointer to the RangeType structure to be classified
## Dependencies
- Functions called/Symbols referenced:
  - [range_get_flags](../r/range_get_flags.md)
  - RANGE_EMPTY (flag constant)
  - CLS_EMPTY (class constant) 
  - RANGE_LB_INF (flag constant)
  - CLS_LOWER_INF (class constant)
  - RANGE_UB_INF (flag constant) 
  - CLS_UPPER_INF (class constant)
  - RANGE_CONTAIN_EMPTY (flag constant)
  - CLS_CONTAIN_EMPTY (class constant)
- Called from:
  - rangeCopy (src/backend/utils/adt/rangetypes_gist.c:180)
  - [range_gist_picksplit](../r/range_gist_picksplit.md) (src/backend/utils/adt/rangetypes_gist.c:652)
  - [range_gist_class_split](../r/range_gist_class_split.md) (src/backend/utils/adt/rangetypes_gist.c:1206)

## Notes and Other Information
- This is a static function used internally within the range types GiST implementation
- The class system allows efficient partitioning of ranges during GiST index construction
- Empty ranges are treated as a special case and cannot have additional properties
- The bit-wise OR operations create unique combinations for different range characteristics

## Simplified Source

```c
static int get_gist_range_class(RangeType *range) {
    int classNumber;
    char flags;

    flags = range_get_flags(range);

    // Empty ranges have a special class
    if (flags & RANGE_EMPTY) {
        classNumber = CLS_EMPTY;
    } else {
        // Build class number by combining property flags
        classNumber = 0;
        if (flags & RANGE_LB_INF)
            classNumber |= CLS_LOWER_INF;
        if (flags & RANGE_UB_INF)
            classNumber |= CLS_UPPER_INF;
        if (flags & RANGE_CONTAIN_EMPTY)
            classNumber |= CLS_CONTAIN_EMPTY;
    }

    return classNumber;
}
```