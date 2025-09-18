# free_db_and_rel_infos

## Location
[src/bin/pg_upgrade/info.c:763-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L763-L778)

## Overview
Frees all database and relation information stored in a DbInfoArr structure, including nested relation arrays and database names.

## Definition


## Detailed Description
This function performs cleanup of a DbInfoArr structure by deallocating all dynamically allocated memory. It iterates through each database in the array, first freeing the relation information for each database using free_rel_infos(), then freeing the database name string. Finally, it deallocates the database array itself and resets the structure to a clean state. This is a critical cleanup function in pg_upgrade to prevent memory leaks during the upgrade process.

## Parameters / Member Variables
- : Pointer to DbInfoArr structure containing database information to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [free_rel_infos](free_rel_infos.md)
  - [pg_free](../p/pg_free.md)
  - [DbInfoArr](../D/DbInfoArr.md) (struct type)
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md)

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/info.c
- Sets db_arr->dbs to NULL and db_arr->ndbs to 0 after cleanup to prevent dangling pointers
- Part of the pg_upgrade utility's memory management system
- Should always be called to clean up DbInfoArr structures to prevent memory leaks