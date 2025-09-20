# RecordIOData

## Location
[src/backend/utils/adt/jsonfuncs.c:227-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L227-L235)

## Overview
RecordIOData is a structure that caches record metadata needed for populating records during JSON processing and general record I/O operations in PostgreSQL.

## Definition

```c
struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
RecordIOData serves as a comprehensive cache structure for record-level metadata in PostgreSQL's type system. It maintains essential information about record types including type identification, the number of columns, and a flexible array of ColumnIOData structures for each column. This structure is fundamental to both JSON processing operations and general record I/O functions, providing efficient access to column metadata without repeated system catalog lookups.

## Parameters / Member Variables
- : OID of the record type being processed
- : Type modifier for the record type, providing additional type-specific information
- : Number of columns in the record type
- : Flexible array of ColumnIOData structures, one for each column in the record, containing cached metadata for individual column processing

## Dependencies
- Functions called/Symbols referenced:
  - [ColumnIOData](../C/ColumnIOData.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [CompositeIOData](../C/CompositeIOData.md)
  - JsObjectFree
  - [populate_record_field](../p/populate_record_field.md)
  - [allocate_record_info](../a/allocate_record_info.md)
  - [populate_record](../p/populate_record.md)
  - [record_in](../r/record_in.md)
  - [record_out](../r/record_out.md)
  - [record_recv](../r/record_recv.md)
  - [record_send](../r/record_send.md)

## Notes and Other Information
- Defined in src/backend/utils/adt/jsonfuncs.c at lines 227-235
- Uses flexible array member (C99 feature) for efficient memory allocation based on actual column count
- Central to PostgreSQL's record processing infrastructure, used in both JSON functions and core record types
- Extensively used in rowtypes.c for fundamental record I/O operations (input, output, receive, send)
- The structure size varies based on the number of columns, making it memory-efficient
- Critical for performance optimization in record-oriented operations by caching column metadata
- Provides the foundation for type-safe record processing across different PostgreSQL subsystems