# getRelationTypeDescription

## Location
[src/backend/catalog/objectaddress.c:4603-4665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4603-L4665)

## Overview
A helper function that determines and appends the specific type description for a relation object to a StringInfo buffer, based on the relation's kind (table, index, view, etc.).

## Definition
```c
static void getRelationTypeDescription(StringInfo buffer, Oid relid, int32 objectSubId, bool missing_ok)
```

## Detailed Description
This function provides detailed type descriptions for relation objects in PostgreSQL. It looks up the relation in the system cache using the provided relation OID, examines the relation's relkind field from pg_class, and appends the appropriate human-readable type description to the provided StringInfo buffer.

The function handles all relation kinds including regular tables, partitioned tables, indexes, sequences, toast tables, views, materialized views, composite types, and foreign tables. For sub-objects (when objectSubId != 0), it appends " column" to indicate that the object refers to a specific column of the relation.

If the relation is not found and missing_ok is false, it throws an error. If missing_ok is true, it falls back to the generic "relation" description.

## Parameters / Member Variables
- `buffer` (StringInfo): StringInfo structure to append the type description to
- `relid` (Oid): Object ID of the relation to describe
- `objectSubId` (int32): Sub-object identifier; if non-zero, indicates a column reference
- `missing_ok` (bool): Whether to tolerate missing relations

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_class
  - Various RELKIND constants (RELKIND_RELATION, RELKIND_INDEX, etc.)
- Called from (representative examples):
  - [getObjectTypeDescription](getObjectTypeDescription.md)
  - object_type_map

## Notes and Other Information
- This is a static helper function, not directly accessible outside objectaddress.c
- Handles all PostgreSQL relation kinds with appropriate descriptive names
- Automatically appends " column" for sub-object references
- Uses system cache for efficient relation metadata lookup
- Falls back gracefully when relations are missing and missing_ok is true
- Located in src/backend/catalog/objectaddress.c:4603-4665