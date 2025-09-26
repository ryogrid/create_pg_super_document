# GinEntries

## Location
[src/backend/utils/adt/jsonb_gin.c:80-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L80-L85)

## Overview
GinEntries is a dynamic buffer structure used to collect and manage GIN index entries during JSONB data processing for both jsonb_ops and jsonb_path_ops operator classes.

## Definition

```c
typedef struct GinEntries
{
	Datum	   *buf;
	int			count;
	int			allocated;
} GinEntries;
```
## Detailed Description
GinEntries implements a resizable array (dynamic buffer) that accumulates GIN index entries as they are generated during JSONB value extraction. It serves as an intermediate collection mechanism before the final array of Datum values is returned to the GIN indexing system.

The structure supports automatic memory management with exponential growth strategy - when the buffer fills up, it doubles in size to accommodate additional entries. This approach minimizes memory reallocations while processing large JSONB documents.

The buffer is used throughout the JSONB GIN extraction process, collecting entries that represent either individual JSON values (for jsonb_ops) or path-aware hash values (for jsonb_path_ops). Once all entries are collected, the final buffer is returned to PostgreSQL's GIN indexing infrastructure.

## Parameters / Member Variables
- : Pointer to dynamically allocated array of Datum values representing the collected GIN entries
- : Current number of entries stored in the buffer
- : Total capacity of the buffer (number of Datum slots allocated)

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL's generic data type)
  - Memory allocation functions (palloc, repalloc)
- Called from (representative examples):
  - init_gin_entries (initialization)
  - add_gin_entry (adding entries)
  - gin_extract_jsonb (main extraction function)
  - gin_extract_jsonb_path (path-based extraction)
  - emit_jsp_gin_entries (JSON path query processing)
  - extract_jsp_query (query extraction)

## Notes and Other Information
- Initial allocation can be specified during initialization, with 0 meaning no pre-allocation
- Default growth size when no initial allocation is 8 entries
- Growth strategy is exponential (doubling) to achieve amortized O(1) append performance
- Memory is managed using PostgreSQL's memory context system (palloc/repalloc)
- The structure is typically used as a local variable and its lifetime is tied to the extraction function's execution context