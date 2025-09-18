# get_tablespace_oid

## Location
src/backend/commands/tablespace.c: 1426 - 1471

## Overview
Looks up the OID of a tablespace by its name, with optional error handling for missing tablespaces.

## Definition


## Detailed Description
This function performs a catalog lookup to find the OID corresponding to a given tablespace name. It searches the pg_tablespace system catalog using a heap scan rather than an index lookup, based on the assumption that most installations have relatively few tablespaces, making a sequential scan more efficient than index overhead.

The function follows a standard PostgreSQL catalog access pattern:
1. **Table access**: Opens pg_tablespace with AccessShareLock
2. **Scan setup**: Initializes a catalog scan with equality condition on spcname column
3. **Result retrieval**: Extracts the OID from the first matching tuple
4. **Cleanup**: Closes the scan and relation
5. **Error handling**: Optionally throws an error if no matching tablespace is found

The missing_ok parameter controls behavior when the tablespace doesn't exist - when false, it throws an ERRCODE_UNDEFINED_OBJECT error; when true, it silently returns InvalidOid.

## Parameters
- : The name of the tablespace to look up
- : If false, throw error for missing tablespace; if true, return InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [table_endscan](../t/table_endscan.md)
  - table_close
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - ForwardScanDirection
  - Form_pg_tablespace
- Called from (representative examples):
  - [GetDefaultTablespace](../G/GetDefaultTablespace.md) (src/backend/commands/tablespace.c:1165)
  - [check_temp_tablespaces](../c/check_temp_tablespaces.md) (src/backend/commands/tablespace.c:1251)
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md) (src/backend/commands/tablespace.c:1385)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:813)
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1286)

## Notes and Other Information
- Uses heap scan instead of index lookup for performance reasons with small pg_tablespace tables
- Assumes at most one matching tuple per tablespace name (enforced by unique constraint)
- Returns InvalidOid when tablespace not found and missing_ok is true
- Widely used throughout PostgreSQL for validating tablespace references in DDL commands
- Essential for translating user-specified tablespace names into internal OID references