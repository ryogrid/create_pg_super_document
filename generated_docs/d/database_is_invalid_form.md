# database_is_invalid_form

## Location
[src/backend/commands/dbcommands.c:3198-3207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3198-L3207)

## Overview
A utility function that checks whether a database is marked as invalid by examining its pg_database catalog entry form.

## Definition

```c
bool
database_is_invalid_form(Form_pg_database datform)
```
## Detailed Description
This function determines if a database is in an invalid state by checking the datconnlimit field in the pg_database catalog entry. When PostgreSQL drops a database, the database row is marked as invalid by setting datconnlimit to DATCONNLIMIT_INVALID_DB, but the catalog contents still exist temporarily. This prevents new connections to the database while the drop operation is in progress. The function provides a clean interface to check this invalid state without directly accessing the catalog structure fields.

## Parameters / Member Variables
- : A pointer to the Form_pg_database structure representing a database catalog entry

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_database (catalog structure type)
  - DATCONNLIMIT_INVALID_DB (constant for invalid database marker)
- Called from (representative examples):
  - [AlterDatabase](../A/AlterDatabase.md) (database alteration operations)
  - [database_is_invalid_oid](database_is_invalid_oid.md) (OID-based invalid check)
  - [vac_truncate_clog](../v/vac_truncate_clog.md) (vacuum operations)
  - [get_database_list](../g/get_database_list.md) (autovacuum daemon)
  - [InitPostgres](../I/InitPostgres.md) (backend initialization)

## Notes and Other Information
- Used during database drop operations to prevent connections to databases being dropped
- The invalid state is temporary - it exists while the database is being physically removed
- This is part of PostgreSQL's safety mechanism to ensure data consistency during database operations
- Returns true only when datconnlimit equals DATCONNLIMIT_INVALID_DB
- Essential for proper database lifecycle management in PostgreSQL