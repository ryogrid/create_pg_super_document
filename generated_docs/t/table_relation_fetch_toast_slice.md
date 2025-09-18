# table_relation_fetch_toast_slice

## Location
[src/include/access/tableam.h:1917-1937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1917-L1937)

## Overview
Fetches all or part of a TOAST value from a TOAST table, providing access to large attribute data stored out-of-line.

## Definition


## Detailed Description
This function provides a table access method interface for retrieving TOAST (The Oversized-Attribute Storage Technique) values or portions thereof from TOAST tables. It supports both full value retrieval and partial slice fetching, which is crucial for efficient handling of large attributes where only a portion of the data may be needed.

The function delegates to the underlying table access method's relation_fetch_toast_slice function, allowing different storage engines to implement their own optimized TOAST retrieval logic. This abstraction is essential because different access methods may organize TOAST data differently and have varying strategies for efficient partial data retrieval.

## Parameters / Member Variables
- : A Relation pointer to the TOAST table containing the stored value
- : OID that identifies which TOAST value to fetch (corresponds to chunk_id in heap tables)
- : Total size of the complete TOAST value being fetched
- : Byte offset within the TOAST value where fetching should begin
- : Number of bytes to fetch from the TOAST value
- : Caller-allocated varlena structure where the fetched bytes will be stored

## Dependencies
- Functions called/Symbols referenced:
  - toastrel->rd_tableam->relation_fetch_toast_slice (table access method function pointer)
  - [varlena](../v/varlena.md) (struct for variable-length data)
- Called from (representative examples):
  - [toast_fetch_datum](toast_fetch_datum.md) (in src/backend/access/common/detoast.c:375)
  - [toast_fetch_datum_slice](toast_fetch_datum_slice.md) (in src/backend/access/common/detoast.c:455)

## Notes and Other Information
- This is an inline function defined in the tableam header file for efficient access
- Part of the table access method abstraction layer for TOAST operations
- Essential for PostgreSQL's ability to efficiently handle large attribute values
- Supports partial data retrieval, which is important for performance when only a subset of large data is needed
- Only required for access methods that can be used to implement TOAST tables
- The result parameter must be pre-allocated by the caller with sufficient space
- Located in src/include/access/tableam.h:1917-1937