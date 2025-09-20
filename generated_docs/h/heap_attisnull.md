# heap_attisnull

## Location
[src/backend/access/common/heaptuple.c:455-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L455-L518)

## Overview
heap_attisnull determines whether a specific attribute (column) in a heap tuple is NULL, handling both regular attributes and system attributes with special logic for missing attributes in tuple descriptors.

## Definition

```c
bool
heap_attisnull(HeapTuple tup, int attnum, TupleDesc tupleDesc)
```
## Detailed Description
heap_attisnull is a fundamental function for checking null values in PostgreSQL heap tuples. It provides a unified interface for testing null values across different types of attributes, including user-defined columns and system attributes. The function handles several special cases:

For regular attributes (attnum > 0):
- Checks if the attribute number exceeds the tuple's actual attribute count
- Handles missing attributes with default values through tuple descriptor metadata
- Uses the tuple's null bitmap to determine null status for present attributes
- Optimizes for tuples with no null values using HeapTupleNoNulls

For system attributes (attnum <= 0):
- System attributes like tableoid, ctid, xmin, xmax, cmin, cmax are never null
- Validates that the system attribute number is recognized

The function also supports cases where tupleDesc is NULL for relations not expected to have missing values (like catalogs and indexes).

## Parameters / Member Variables
- : HeapTuple structure containing the tuple data to examine
- : Attribute number to check (1-based for user attributes, negative for system attributes)
- : TupleDesc describing the tuple structure (can be NULL for some relation types)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetNatts (get number of attributes in tuple)
  - TupleDescAttr (access tuple descriptor attribute info)
  - HeapTupleNoNulls (check if tuple has any null values)
  - att_isnull (check specific bit in null bitmap)
  - TableOidAttributeNumber, SelfItemPointerAttributeNumber, etc. (system attribute constants)
  - elog (error logging)
- Called from (representative examples):
  - [ExecEvalRowNullInt](../E/ExecEvalRowNullInt.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [transformFkeyCheckAttrs](../t/transformFkeyCheckAttrs.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Attribute numbers are 1-based for user attributes, with system attributes using negative numbers
- The function handles the PostgreSQL feature of missing attributes with default values
- Optimizes performance by checking HeapTupleNoNulls before examining the null bitmap
- System attributes (tableoid, ctid, transaction IDs, command IDs) are always non-null by design
- Supports NULL tupleDesc for catalog relations and indexes that don't have missing attributes
- Critical for SQL NULL handling throughout the PostgreSQL query execution system
- Used extensively in index operations, constraint checking, and query evaluation
- Part of the core tuple access interface used by virtually all tuple-processing code