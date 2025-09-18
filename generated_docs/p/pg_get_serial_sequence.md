# pg_get_serial_sequence

## Location
src/backend/utils/adt/ruleutils.c: 2787 - 2880

## Overview
Retrieves the fully qualified name of the sequence associated with a serial or identity column, formatted for use with sequence manipulation functions.

## Definition
```c
Datum pg_get_serial_sequence(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_get_serial_sequence is a PostgreSQL system function that finds the sequence object associated with a specific column in a table, particularly for SERIAL or IDENTITY columns. It takes a table name and column name as parameters, then searches the dependency catalog (pg_depend) to locate the sequence that has an automatic or internal dependency relationship with the specified column.

The function performs several steps: it resolves the table name to an OID, validates that the specified column exists, searches the pg_depend system catalog for dependencies, and filters for sequences with the appropriate dependency type (DEPENDENCY_AUTO for SERIAL columns or DEPENDENCY_INTERNAL for IDENTITY columns). If a matching sequence is found, it returns the sequence's fully qualified name in a format suitable for use with setval(), nextval(), or currval() functions.

## Parameters / Member Variables
- `tablename`: TEXT containing the name of the table (can be schema-qualified)
- `columnname`: TEXT containing the name of the column (treated as double-quoted identifier)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting TEXT arguments)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (creates RangeVar from qualified name list)
  - textToQualifiedNameList (parses text into qualified name components)
  - RangeVarGetRelid (resolves RangeVar to relation OID)
  - text_to_cstring (converts TEXT to C string)
  - [get_attnum](../g/get_attnum.md) (gets attribute number for column name)
  - table_open (opens system table with lock)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan keys for system catalog search)
  - [systable_beginscan](../s/systable_beginscan.md) (begins system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (gets next tuple from system scan)
  - [systable_endscan](../s/systable_endscan.md) (ends system catalog scan)
  - table_close (closes system table and releases lock)
  - [get_rel_relkind](../g/get_rel_relkind.md) (gets relation kind for OID)
  - generate_qualified_relation_name (creates qualified name for relation)
  - string_to_text (converts C string to TEXT)
  - PG_RETURN_TEXT_P (macro for returning TEXT result)
- Called from:
  - SQL function pg_get_serial_sequence() available to users

## Notes and Other Information
- This function is exposed as a SQL-callable system function in PostgreSQL
- Returns NULL if no associated sequence is found or if the column is not a serial/identity column
- The first parameter (table name) is not treated as double-quoted, while the second (column name) is double-quoted
- Searches for both DEPENDENCY_AUTO (SERIAL columns) and DEPENDENCY_INTERNAL (IDENTITY columns)
- Does not lock the target table during lookup to avoid privilege issues
- Uses AccessShareLock when scanning the dependency table
- Located in src/backend/utils/adt/ruleutils.c:2787-2880
- The returned sequence name is fully schema-qualified for unambiguous reference