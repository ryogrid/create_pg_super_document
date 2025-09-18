# brin_minmax_multi_add_value

## Location
src/backend/access/brin/brin_minmax_multi.c: 2412 - 2548

## Overview
Adds a new value to BRIN minmax multi-column index summaries, expanding the range coverage and updating index tuples when necessary.

## Definition
```c
Datum brin_minmax_multi_add_value(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the core value insertion mechanism for BRIN minmax multi-column indexes. It examines index tuples containing partial range summaries and determines whether a new heap tuple value requires updating the index. The function handles three main scenarios:

1. **First Value Insertion**: When the column contains only nulls (`bv_allnulls` is true), initializes a new range structure with an optimized buffer size for batch operations. The buffer size is calculated as 10x the target maximum values, capped by heap constraints.

2. **Deserialization from Storage**: When working with serialized range data from disk, deserializes the ranges into memory-resident structures and sets up appropriate buffer sizes for continued processing.

3. **Value Addition**: Attempts to add the new value to existing ranges using `range_add_value`, which may extend existing ranges or create new ones.

The function employs intelligent memory management, using larger buffers during batch operations to minimize memory allocations and improve performance. Buffer sizes are calculated based on BRIN range size, heap pages per range, and configurable limits.

## Parameters / Member Variables
- `bdesc`: BRIN descriptor containing index metadata and configuration
- `column`: BrinValues structure representing the column being indexed
- `newval`: New datum value to be added to the range summary
- `isnull`: Boolean indicating if the new value is null (unused due to assertion)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER/PG_GETARG_DATUM: Extract function arguments
  - PG_GET_OPCLASS_OPTIONS/PG_GET_COLLATION: Get operator class options and collation
  - TupleDescAttr: Access tuple descriptor attributes  
  - brin_minmax_multi_get_values: Get configured values per range
  - minmax_multi_init: Initialize new range structures
  - brin_range_deserialize: Convert serialized ranges to memory format
  - minmax_multi_get_strategy_procinfo: Get comparison function info
  - range_add_value: Add value to existing ranges
  - brin_minmax_multi_serialize: Set serialization function pointer
- Called from (representative examples):
  - Not directly referenced by other symbols (likely called through function pointer mechanism)

## Notes and Other Information
- Returns boolean indicating whether the index tuple was modified
- Uses dynamic buffer sizing with factors and caps for optimal performance:
  - Buffer factor: 10x target values
  - Minimum buffer: MINMAX_BUFFER_MIN
  - Maximum buffer: MINMAX_BUFFER_MAX
- Handles both initial range creation and existing range updates
- Memory operations are performed in the appropriate memory context (`bv_context`)
- Sets up serialization function pointer for later storage operations
- Critical component in BRIN index maintenance and query optimization
- Assertion ensures non-null values are processed correctly