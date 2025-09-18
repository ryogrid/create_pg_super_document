# _becomeOwner

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3439-3454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3439-L3454)

## Overview
Changes the session authorization to the owner of a given TOC (Table of Contents) entry object during PostgreSQL database restore operations.

## Definition


## Detailed Description
The  function is a utility function in pg_dump's archiver that handles ownership changes during database restore operations. It conditionally switches the current session user to match the owner of a specific database object being restored. This ensures that objects are created with the correct ownership during the restore process.

The function includes safety checks to respect restore options that may disable ownership changes () or session authorization usage (). If either of these options is set, the function returns early without making any changes.

## Parameters / Member Variables
- : Archive handle containing restore options and database connection information
- : TOC entry representing the database object whose owner should be assumed

## Dependencies
- Functions called/Symbols referenced:
  -  - Actually performs the session authorization change
- Data types referenced:
  -  - Structure representing a database object in the restore archive
  -  - Structure containing restore configuration options
- Called from (representative examples):
  -  - Main restore function
  -  - Individual object restore function  
  -  - TOC entry output function

## Notes and Other Information
- This is a static function, only accessible within the pg_backup_archiver.c file
- The function respects the  and  restore options to provide flexibility in restore behavior
- Part of PostgreSQL's pg_dump/pg_restore infrastructure for database backup and restore operations
- The actual session authorization change is delegated to the  helper function