# expanded_record_get_field

## Location
[src/include/utils/expandedrecord.h:228-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/expandedrecord.h#L228-L241)

## Overview
Retrieves the value of a specific field from an expanded record, with optimized fast path access when field data is cached.

## Definition

```c
static inline Datum
expanded_record_get_field(ExpandedRecordHeader *erh, int fnumber,
						  bool *isnull)
```
## Detailed Description
This inline function provides efficient access to individual fields within an expanded record. It implements a two-tier optimization strategy:

1. **Fast path**: If the expanded record has valid cached field data (indicated by the ER_FLAG_DVALUES_VALID flag) and the field number is within bounds, it directly accesses the pre-computed dvalues and dnulls arrays using array indexing. This path uses  branch prediction since field accesses typically hit the cache.

2. **Slow path**: If the field data is not cached or the field number is out of bounds, it falls back to  which will extract the field from the flat tuple representation or construct it as needed.

The function converts from 1-based field numbering (PostgreSQL convention) to 0-based array indexing for internal access.

## Parameters / Member Variables
- : Pointer to an ExpandedRecordHeader structure containing the record data
- : 1-based field number to retrieve (must be > 0 and <= number of fields)
- : Output parameter set to true if the field value is NULL, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - [expanded_record_fetch_field](expanded_record_fetch_field.md)
  - ExpandedRecordHeader
- Called from (representative examples):
  - [ExecEvalFieldSelect](../E/ExecEvalFieldSelect.md)

## Notes and Other Information
- This is an inline function for maximum performance in field access operations
- Uses branch prediction optimization with  since field data is typically cached
- Part of PostgreSQL's expanded object infrastructure for efficient composite type field access
- The fast path requires the ER_FLAG_DVALUES_VALID flag to be set in the record header
- Field numbering follows PostgreSQL convention (1-based) but internally converts to 0-based array access
- Commonly used in expression evaluation for field selection operations
- Located in src/include/utils/expandedrecord.h:228-241