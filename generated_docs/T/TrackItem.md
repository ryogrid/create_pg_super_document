# TrackItem

## Location
[src/backend/commands/analyze.c:2032-2355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L2032-L2355)

## Overview
A local structure used within statistical analysis functions to track distinct values and their occurrence counts during sample data processing.

## Definition
```c
typedef struct
{
    Datum    value;
    int      count;
} TrackItem;
```

## Detailed Description
The `TrackItem` structure is a simple container used internally by PostgreSQL's statistical analysis functions to maintain a list of distinct values encountered during sample data processing along with their frequency counts. This structure is fundamental to building Most Common Values (MCV) lists and computing distinct value statistics. It serves as the basic building block for tracking value frequencies before they are processed into final statistical summaries. The structure is typically used in dynamically allocated arrays that grow as new distinct values are encountered during the analysis phase.

## Parameters / Member Variables
- `value`: A Datum containing the distinct value being tracked
- `count`: Integer representing the number of times this value has been encountered in the sample

## Dependencies
- Functions called/Symbols referenced:
  - None (structure definition)
- Called from (representative examples):
  - [compute_scalar_stats](../c/compute_scalar_stats.md) (implicitly through local typedef)
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md)
  - [compute_array_stats](../c/compute_array_stats.md)

## Notes and Other Information
- This is a local typedef typically defined within statistical analysis functions
- Used extensively in MCV (Most Common Values) list construction
- The structure is allocated in arrays with typical sizes of 2*n for n-element MCV lists, with a minimum of 10 elements
- Values stored as Datum type to support any PostgreSQL data type
- Count field tracks frequency for statistical significance determination
- Used in conjunction with sorting and pruning algorithms to identify the most frequent values
- Essential component of PostgreSQL's ANALYZE operation for generating column statistics
- Different analysis modules (tsvector, array, etc.) use similar structures for their specific data types