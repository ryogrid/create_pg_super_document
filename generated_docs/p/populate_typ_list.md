# populate_typ_list

## Location
[src/backend/bootstrap/bootstrap.c:695-734](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L695-L734)

## Overview
A static function in the PostgreSQL bootstrap module that loads the global Typ list by reading all type definitions from the pg_type system catalog.

## Definition

```c
struct typmap *newtyp;
```
## Detailed Description
This function initializes the global Typ list (a linked list) by scanning the entire pg_type system catalog and creating a typmap structure for each type definition found. It is called during the bootstrap process to build an in-memory cache of type information that can be quickly accessed during subsequent operations.

The function performs a full table scan of the pg_type relation, extracts the type form data from each tuple, and creates corresponding typmap entries that store both the OID and the complete type structure. All memory allocations are done in TopMemoryContext to ensure the type list persists throughout the bootstrap session.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (opens the pg_type relation)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md) (begins catalog scan)
  - [heap_getnext](../h/heap_getnext.md) (retrieves next tuple from scan)
  - [table_endscan](../t/table_endscan.md) (ends the table scan)
  - [table_close](../t/table_close.md) (closes the relation)
  - ForwardScanDirection (scan direction constant)
  - Form_pg_type (type form structure)
  - [typmap](../t/typmap.md) (type mapping structure)
  - [TableScanDesc](../T/TableScanDesc.md) (table scan descriptor type)

- Called from:
  - [boot_openrel](../b/boot_openrel.md) (during bootstrap relation opening)
  - [gettype](../g/gettype.md) (when type lookup is needed)

## Notes and Other Information
- Asserts that the global Typ list is NIL (empty) before populating it
- Uses TopMemoryContext to ensure type list entries persist beyond function scope
- The populated Typ list serves as a cache for type lookups during bootstrap operations
- Each typmap entry contains both the type OID (am_oid) and a complete copy of the type structure (am_typ)
- This function is critical for bootstrap performance as it avoids repeated pg_type lookups