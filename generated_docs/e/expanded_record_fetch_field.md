# expanded_record_fetch_field

## Location
[src/backend/utils/adt/expandedrecord.c:1063-1111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L1063-L1111)

## Overview
Retrieves the value of a specific field from an expanded record by field number, handling both user-defined fields and system columns.

## Definition

```c
structed form */
		deconstruct_expanded_record(erh);
```
## Detailed Description
This function serves as the backend implementation for field access in expanded records, handling cases that cannot be optimized by the inline expanded_record_get_field function. It provides comprehensive field access including edge cases and system columns.

The function operates differently based on the field number:

For positive field numbers (user-defined fields):
1. First checks if the record is empty using ExpandedRecordIsEmpty - empty records return null for all fields
2. Ensures the record is deconstructed by calling deconstruct_expanded_record to populate the dvalues/dnulls arrays
3. Performs bounds checking - out-of-range field numbers return null
4. Returns the corresponding value from the dvalues array and null flag from dnulls array

For zero or negative field numbers (system columns):
1. Checks if a flat tuple (fvalue) exists - system columns require a materialized tuple
2. If no flat tuple exists, returns null
3. Otherwise delegates to heap_getsysattr to extract the system column value

The function is designed as the "slow path" complement to the inline expanded_record_get_field, which handles the common fast case when dvalues are already valid.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader containing the record
- : Field number to fetch (positive for user fields, zero/negative for system columns)
- : Output parameter - set to true if the field value is null

## Dependencies
- Functions called/Symbols referenced:
  - ExpandedRecordIsEmpty
  - [deconstruct_expanded_record](../d/deconstruct_expanded_record.md)
  - [heap_getsysattr](../h/heap_getsysattr.md)
- Types referenced:
  - ExpandedRecordHeader
  - Datum
- Macros used:
  - unlikely (for performance optimization)
- Called from (representative examples):
  - [expanded_record_get_field](expanded_record_get_field.md) (inline function in header)

## Notes and Other Information
- This function handles the "slow path" for field access when optimizations cannot be applied
- System columns (fnumber <= 0) include oid, tableoid, xmin, xmax, cmin, cmax, ctid
- Out-of-bounds field access is safe and returns null rather than erroring
- The function ensures proper bounds checking and handles empty records gracefully
- Performance is optimized for the common case through the inline wrapper function
- System column access requires a materialized tuple (fvalue) to be present