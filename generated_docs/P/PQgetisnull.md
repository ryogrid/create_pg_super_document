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
  - check_tuple_field_number
  - NULL_LEN
- Called from (representative examples):
  - libpqrcv_create_slot
  - libpqrcv_processTuples
  - verify_heap_slot_handler
  - compile_database_list
  - compile_relation_list_one_db
  - BaseBackup
  - dumpTableData_insert
  - dumpDatabase
  - dumpRoles
  - run_simple_query
  - StoreQueryTuple
  - printQuery
  - ecpg_get_data

## Notes and Other Information
- Returns 1 (true) if the field value is NULL or if the tuple/field number is invalid
- Returns 0 (false) if the field contains actual data (including empty strings)
- Essential companion function to PQgetvalue() for proper NULL handling
- Used extensively throughout PostgreSQL tools (pg_dump, psql, pg_rewind, etc.)
- Invalid field references are treated as NULL for defensive programming
- The NULL_LEN constant (-1) is used internally to mark NULL database values