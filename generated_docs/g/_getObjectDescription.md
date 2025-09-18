# _getObjectDescription

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3664 - 3757

## Overview
Extracts an object description for a TOC entry and appends it to a buffer, primarily used for generating ALTER ... OWNER TO statements in pg_dump operations.

## Definition


## Detailed Description
This function builds appropriate object descriptions for different PostgreSQL database objects based on their type. It handles three main categories of objects:

1. **Objects with simple decoration**: Tables, views, sequences, domains, etc. - formatted as "TYPE [schema.]name"
2. **Objects requiring complex decoration**: Aggregates, functions, operators, procedures - uses information from DROP statements
3. **Objects without owners**: Constraints, indexes, triggers, etc. - no description is generated

The function uses string comparisons to determine the object type from the TOC entry's description field and formats the output accordingly. For objects that require schema qualification, it includes the namespace prefix when available.

## Parameters / Member Variables
- : PQExpBuffer to append the object description to
- : Pointer to TocEntry structure containing object metadata including type (desc), name (tag), namespace, and drop statement

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry (struct type)
  - fmtId (for identifier quoting)
  - appendPQExpBuffer (for formatted string appending)
  - appendPQExpBufferStr (for string appending)
  - pg_strdup (for string duplication)
  - pg_fatal (for error reporting)
  - strcmp (for string comparison)
- Called from:
  - _printTocEntry (main caller for generating owner change statements)

## Notes and Other Information
- Function is static and only used within pg_backup_archiver.c
- Handles a comprehensive list of PostgreSQL object types including newer additions like publications and subscriptions
- For complex objects like functions and operators, it cleverly reuses the DROP statement syntax by removing the "DROP " prefix
- Objects without owners (constraints, indexes, etc.) result in no output, which is correct behavior since they inherit ownership from their parent objects
- Large objects (BLOBs) receive special formatting as "LARGE OBJECT" followed by their numeric identifier