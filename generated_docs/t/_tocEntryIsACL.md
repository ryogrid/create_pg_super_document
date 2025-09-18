# _tocEntryIsACL

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3240 - 3254

## Overview
A static utility function that identifies whether a given Table of Contents (TOC) entry represents an Access Control List (ACL) in PostgreSQL's pg_dump/pg_restore functionality.

## Definition
```c
static bool _tocEntryIsACL(TocEntry *te)
```

## Detailed Description
This function serves as a centralized check to determine if a TOC entry corresponds to an ACL-related database object. It examines the description field of the TOC entry and matches against known ACL-related descriptors. The function was designed to avoid hard-wired assumptions about which entries are restored during the RESTORE_PASS_ACL phase, providing flexibility in ACL handling logic.

The function checks for three specific ACL types:
- Standard "ACL" entries
- "ACL LANGUAGE" entries (legacy from PostgreSQL 7.4, noted as a "crock" in comments)  
- "DEFAULT ACL" entries for default privileges

## Parameters / Member Variables
- `te`: Pointer to a TocEntry structure containing the TOC entry to examine

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry (struct type)
  - strcmp (standard C library function)
- Called from (representative examples):
  - _tocEntryRequired
  - _printTocEntry

## Notes and Other Information
- Located in src/bin/pg_dump/pg_backup_archiver.c:3240-3254
- The "ACL LANGUAGE" check is maintained for backward compatibility with PostgreSQL 7.4 dumps, though it's considered deprecated
- This function helps maintain separation of concerns between ACL identification and restoration phases
- Part of the pg_dump/pg_restore utility's archiver component