# DropLOIfExists

## Location
[src/bin/pg_dump/pg_backup_db.c:673-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L673-L680)

## Overview
DropLOIfExists generates SQL to conditionally drop a large object if it exists, used during pg_restore operations to handle existing large objects.

## Definition


## Detailed Description
This function generates a SQL query that safely removes a large object (LO) if it exists in the database. It uses PostgreSQL's pg_catalog.lo_unlink() function to delete the large object, but only if the specified OID exists in the pg_largeobject_metadata system catalog.

The generated SQL performs a conditional delete by selecting from pg_largeobject_metadata and calling lo_unlink() for the matching OID. This approach avoids errors that would occur from attempting to delete non-existent large objects during restoration operations.

The function is typically used in pg_restore scenarios where large objects may already exist and need to be replaced, or where the restore process needs to handle potential conflicts gracefully.

## Parameters / Member Variables
- : Archive handle used for output formatting and database connection context
- : The OID (Object Identifier) of the large object to be dropped

## Dependencies
- Functions called/Symbols referenced:
  - [ahprintf](../a/ahprintf.md)
- Types referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - Oid

- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md)
  - [StartRestoreLO](../S/StartRestoreLO.md)
  - [_StartLO](../S/_StartLO.md)

## Notes and Other Information
- This is a simple utility function that generates SQL rather than executing it directly
- The generated SQL uses PostgreSQL's system catalogs (pg_catalog.pg_largeobject_metadata) to ensure the large object exists before attempting deletion
- The function uses lo_unlink() which is PostgreSQL's built-in function for removing large objects
- The SQL output includes proper formatting with newline termination
- This function is part of the pg_dump/pg_restore infrastructure for handling large objects during database backup and restoration
- File location: src/bin/pg_dump/pg_backup_db.c:673-680