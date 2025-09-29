# get_table_am_oid

## Location
[src/backend/commands/amcmds.c:173-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L173-L182)

## Overview
Looks up an access method by name and verifies it corresponds to a table access method, returning its OID.

## Definition

```c
Oid
get_table_am_oid(const char *amname, bool missing_ok)
```
## Detailed Description
get_table_am_oid is a specialized wrapper function that provides type-safe lookup of table access methods. It uses the internal get_am_type_oid function with the AMTYPE_TABLE constraint to ensure that only valid table access methods are returned. This function is essential for table creation and modification operations where the storage access method must be validated and resolved to its corresponding OID.

## Parameters / Member Variables
- : Name of the table access method to look up
- : If false, throws error when access method not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - [get_am_type_oid](get_am_type_oid.md): Internal worker function for access method lookup
  - AMTYPE_TABLE: Constant defining the table access method type
- Called from (representative examples):
  - [check_default_table_access_method](../c/check_default_table_access_method.md): Validates default table access method settings
  - [DefineRelation](../D/DefineRelation.md): Table creation processing
  - [ATPrepSetAccessMethod](../A/ATPrepSetAccessMethod.md): ALTER TABLE SET ACCESS METHOD preparation

## Notes and Other Information
- Provides type-safe interface specifically for table access methods
- Thin wrapper around get_am_type_oid with AMTYPE_TABLE constraint
- Critical for table storage layer initialization and modification
- Ensures that only table-compatible access methods are accepted
- Used in both table creation and ALTER TABLE operations
- Location: src/backend/commands/amcmds.c:173-182

## Simplified Source

```c
Oid
get_table_am_oid(const char *amname, bool missing_ok)
{
    // Delegate to generic access method lookup with table type constraint
    return get_am_type_oid(amname, AMTYPE_TABLE, missing_ok);
}
```