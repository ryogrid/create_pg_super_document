# find_typed_table_dependencies

## Location
src/backend/commands/tablecmds.c: 6896 - 6944

## Overview
Identifies typed tables that depend on a specified composite type and either returns their list or raises an error based on the specified drop behavior.

## Definition
```c
static List *find_typed_table_dependencies(Oid typeOid, const char *typeName, DropBehavior behavior)
```

## Detailed Description
This function searches for typed tables that use a specific composite type as their row type by scanning the pg_class system catalog. Typed tables are tables created with the OF clause that inherit their structure from a composite type. The function's behavior depends on the DropBehavior parameter: if RESTRICT is specified, it immediately raises an error when any dependent typed tables are found; otherwise, it collects and returns a list of the OIDs of all dependent typed tables.

The function performs a catalog scan on pg_class using the reloftype attribute to find relations that are typed with the specified type OID. This is essential for operations like ALTER TYPE that need to either prevent changes when dependent objects exist (RESTRICT behavior) or propagate changes to dependent typed tables (CASCADE behavior).

## Parameters / Member Variables
- `typeOid`: The OID of the composite type to check for typed table dependencies
- `typeName`: The name of the type, used in error messages when RESTRICT behavior is specified
- `behavior`: The drop behavior determining whether to error (DROP_RESTRICT) or collect dependencies

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - table_beginscan_catalog
  - heap_getnext
  - table_endscan
  - table_close
  - lappend_oid
  - ereport
  - Form_pg_class
  - DropBehavior
  - DROP_RESTRICT
- Called from (representative examples):
  - child_dependency_type
  - renameatt_internal
  - ATTypedTableRecursion

## Notes and Other Information
- The function is static, indicating it's only used within the tablecmds.c file
- Returns NIL (empty list) if no typed tables depend on the specified type
- The error message suggests using CASCADE to handle dependent typed tables
- Uses AccessShareLock for safe concurrent access to the pg_class catalog
- Typed tables are a PostgreSQL feature allowing tables to inherit structure from composite types