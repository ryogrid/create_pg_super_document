# PQgetisnull

## Location
src/interfaces/libpq/fe-exec.c: 3901 - 3914

## Overview
PQgetisnull tests whether a field value in a PostgreSQL query result is NULL, returning 1 if NULL and 0 if not NULL.

## Definition
```c
int PQgetisnull(const PGresult *res, int tup_num, int field_num)
```

## Detailed Description
PQgetisnull is essential for distinguishing between actual NULL database values and empty strings in PostgreSQL query results. Since PQgetvalue() returns an empty string for both NULL values and actual empty strings, this function provides the only reliable way to detect NULL values in the result set.

The function performs bounds checking using check_tuple_field_number() before examining the field. If the bounds check fails (invalid tuple or field number), the function returns 1, treating invalid references as NULL for safety. For valid fields, it checks if the length is set to NULL_LEN (-1), which is the internal marker for NULL database values.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure containing the query results  
- `tup_num`: Zero-based row number (tuple index) to check
- `field_num`: Zero-based column number (field index) to check

## Dependencies
- Functions called/Symbols referenced:
  - [check_tuple_field_number](../c/check_tuple_field_number.md)
  - NULL_LEN
- Called from (representative examples):
  - [libpqrcv_create_slot](../l/libpqrcv_create_slot.md)
  - [libpqrcv_processTuples](../l/libpqrcv_processTuples.md)
  - [verify_heap_slot_handler](../v/verify_heap_slot_handler.md)
  - [compile_database_list](../c/compile_database_list.md)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md)
  - [BaseBackup](../B/BaseBackup.md)
  - [dumpTableData_insert](../d/dumpTableData_insert.md)
  - [dumpDatabase](../d/dumpDatabase.md)
  - [dumpRoles](../d/dumpRoles.md)
  - [run_simple_query](../r/run_simple_query.md)
  - [StoreQueryTuple](../S/StoreQueryTuple.md)
  - [printQuery](../p/printQuery.md)
  - ecpg_get_data

## Notes and Other Information
- Returns 1 (true) if the field value is NULL or if the tuple/field number is invalid
- Returns 0 (false) if the field contains actual data (including empty strings)
- Essential companion function to PQgetvalue() for proper NULL handling
- Used extensively throughout PostgreSQL tools (pg_dump, psql, pg_rewind, etc.)
- Invalid field references are treated as NULL for defensive programming
- The NULL_LEN constant (-1) is used internally to mark NULL database values