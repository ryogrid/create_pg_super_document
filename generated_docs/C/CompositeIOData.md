# CompositeIOData

## Location
[src/backend/utils/adt/jsonfuncs.c:173-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L173-L186)

## Overview
CompositeIOData is a structure used to cache metadata needed for populating composite types during JSON processing operations in PostgreSQL.

## Definition


## Detailed Description
CompositeIOData serves as a comprehensive metadata cache for composite type handling in JSON functions. It maintains essential information about composite types including a pointer to RecordIOData for record population, a cached tuple descriptor, and specific handling for domain types over composites. The structure is designed to optimize performance by avoiding repeated lookups of type information during JSON-to-composite-type conversions.

## Parameters / Member Variables
- : Pointer to RecordIOData structure containing metadata cache for populate_record() operations
- : Cached TupleDesc (tuple descriptor) providing schema information for the composite type
- : OID of the base type, differs from target type only when dealing with domains over composite types
- : Type modifier for the base type, used for domain-over-composite scenarios
- : Opaque pointer to cached domain constraint checking information, used only for domain types over composites

## Dependencies
- Functions called/Symbols referenced:
  - [RecordIOData](../R/RecordIOData.md)
- Called from (representative examples):
  - [ColumnIOData](ColumnIOData.md)
  - JsObjectFree
  - [update_cached_tupdesc](../u/update_cached_tupdesc.md)
  - [populate_composite](../p/populate_composite.md)

## Notes and Other Information
- Defined in src/backend/utils/adt/jsonfuncs.c at lines 173-186
- Uses pointer to RecordIOData due to variable-length constraints in ColumnIOData.io union
- Special handling for domain types over composite types through base_typid, base_typmod, and domain_info fields
- Part of PostgreSQL's JSON infrastructure for efficient composite type processing
- The structure supports both direct composite types and domains defined over composite types