# ExecFilterJunk

## Location
[src/backend/executor/execJunk.c:247-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execJunk.c#L247-L304)

## Overview
Constructs and returns a "clean" tuple slot with all junk attributes removed from the input tuple, using the mapping information stored in the JunkFilter to transpose only the non-junk attributes.

## Definition
```c
TupleTableSlot *ExecFilterJunk(JunkFilter *junkfilter, TupleTableSlot *slot)
```

## Detailed Description
ExecFilterJunk performs the actual filtering operation that removes junk attributes from a tuple, producing a "clean" tuple suitable for output, storage, or further processing. This function is the culmination of the junk filtering system, using the mapping created during JunkFilter initialization to efficiently project only the desired attributes.

The function operates in several stages:
1. **Extraction**: Retrieves all attribute values from the input tuple using slot_getallattrs()
2. **Preparation**: Clears the result slot and prepares value/null arrays for the clean tuple
3. **Transposition**: Uses the cleanMap array to copy values from original positions to clean positions
4. **Handling Special Cases**: Maps zero entries (deleted columns) to NULL values
5. **Materialization**: Creates a virtual tuple containing only the filtered attributes

The mapping logic handles both regular attribute filtering and deleted column scenarios. When cleanMap[i] is zero, it indicates a deleted column that should be represented as NULL in the output.

## Parameters / Member Variables
- `junkfilter`: Initialized JunkFilter containing the mapping and result slot information
- `slot`: Input TupleTableSlot containing the original tuple with junk attributes

## Dependencies
- Functions called/Symbols referenced:
  - slot_getallattrs: Ensures all attributes are materialized in the input slot
  - ExecClearTuple: Clears the result slot before building new tuple
  - ExecStoreVirtualTuple: Materializes the filtered tuple as a virtual tuple
  - JunkFilter: Input structure containing mapping and result slot
- Called from (representative examples):
  - ExecEvalWholeRowVar: For whole-row variable evaluation with junk filtering
  - ExecutePlan: During main query execution when producing final results
  - sqlfunction_receive: In SQL function execution contexts

## Notes and Other Information
- Returns a virtual tuple stored in the JunkFilter's result slot for efficiency
- The cleanMap array uses 1-based indexing (j-1 when accessing old_values array)
- Zero entries in cleanMap produce NULL values, supporting deleted column scenarios
- Does not modify the input slot, creating a new filtered representation
- Critical for ensuring that internal executor attributes don't leak into query results
- The result slot is reused across calls for the same JunkFilter, improving performance
- Part of PostgreSQL's mechanism for cleanly separating internal processing attributes from user-visible data