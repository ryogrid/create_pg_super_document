# pgstat_release_db_entry_refs

## Location
[src/backend/utils/activity/pgstat_shmem.c:787-800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L787-L800)

## Overview
Releases all local references to shared statistics entries that belong to a specific database, discarding any pending updates.

## Definition

```c
static void
pgstat_release_db_entry_refs(Oid dboid)
```
## Detailed Description
This function provides a database-specific cleanup mechanism for releasing local references to shared statistics entries. When a database is being dropped or when database-specific cleanup is needed, this function ensures that all local references to statistics entries belonging to that database are properly released.

The function uses the  callback to filter entries by database OID and calls  with the  flag set to true. This means any pending statistics updates for the specified database will be discarded rather than flushed to shared memory before releasing the references.

## Parameters / Member Variables
- : The OID of the database for which all local entry references should be released

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs selective reference release based on matching criteria
  - : Callback function used to match entries by database OID
  - : Macro to convert an OID to a Datum for passing to the match function

- Called from (representative examples):
  - : Called during database drop operations to clean up statistics references

## Notes and Other Information
- This is a static function used internally within the statistics shared memory module
- The function always sets  to true, meaning pending updates are discarded rather than flushed
- This behavior is appropriate for database drop scenarios where the statistics updates are no longer needed
- The function leverages the callback mechanism to efficiently filter entries by database
- Part of PostgreSQL's cleanup mechanism to prevent memory leaks when databases are dropped
- Ensures that shared memory resources can be properly freed when databases are removed