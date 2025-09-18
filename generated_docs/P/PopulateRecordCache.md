# PopulateRecordCache

## Location
[src/backend/utils/adt/jsonfuncs.c:236-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L236-L241)

## Overview
A per-query cache structure used by PostgreSQL's JSON populate_record_worker and populate_recordset_worker functions to optimize record population operations by caching metadata.

## Definition


## Detailed Description
PopulateRecordCache is a caching structure designed to improve performance when populating PostgreSQL records from JSON data. It stores essential metadata that would otherwise need to be recomputed for each record operation. The cache is particularly beneficial when processing multiple records of the same type, as it allows the system to reuse type information and column metadata across multiple populate operations.

The structure is stored in a specific memory context (fn_mcxt) to ensure proper memory management and cleanup. It works in conjunction with the populate_record_worker and populate_recordset_worker functions to efficiently convert JSON data into PostgreSQL composite types.

## Parameters / Member Variables
- : Object identifier (Oid) representing the declared type of the record argument being populated
- : ColumnIOData structure containing cached metadata for the populate_composite() function, including column type information and conversion details
- : Memory context where this cache structure is allocated and stored, ensuring proper memory lifecycle management

## Dependencies
- Functions called/Symbols referenced:
  - [ColumnIOData](../C/ColumnIOData.md) (struct type for column metadata caching)
- Called from (representative examples):
  - [PopulateRecordsetState](PopulateRecordsetState.md) (contains this as a member)
  - JsObjectFree (references for cleanup)
  - [get_record_type_from_argument](../g/get_record_type_from_argument.md)
  - [get_record_type_from_query](../g/get_record_type_from_query.md)
  - [populate_record_worker](../p/populate_record_worker.md)
  - [populate_recordset_record](../p/populate_recordset_record.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)

## Notes and Other Information
- This cache is essential for performance optimization in JSON-to-record conversion operations
- The cache is allocated in a specific memory context to ensure proper cleanup when the query completes
- Used primarily in src/backend/utils/adt/jsonfuncs.c for JSON processing functionality
- The cache helps avoid repeated type lookups and metadata computation when processing multiple records of the same type