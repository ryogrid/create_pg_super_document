# _collInfo

## Location
src/bin/pg_dump/pg_dump.h: 282 - 286

## Overview
A structure definition used in PostgreSQL's pg_dump utility to represent collation information for database dumping and restoration operations.

## Definition


## Detailed Description
The  structure is part of PostgreSQL's pg_dump utility framework, designed to store metadata about collations during database backup operations. Collations in PostgreSQL define the rules for sorting and comparing character data, including locale-specific sorting rules, case sensitivity, and character equivalence rules. This structure extends the base  to include collation-specific information, enabling pg_dump to properly serialize and restore collation definitions along with their encoding and ownership information.

## Parameters / Member Variables
- : Base  structure containing common metadata for dumpable database objects (object ID, name, namespace, dump flags, etc.)
- : Pointer to constant string containing the name of the role (user) who owns this collation
- : Integer representing the character encoding this collation is associated with (e.g., UTF8, LATIN1, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
- Called from (representative examples):
  - getCollations (allocation and initialization of collation arrays)
  - dumpCollation (for dumping collation definitions)
  - findCollationByOid (for collation lookup operations)
  - Comparison functions in pg_dump_sort.c (for sorting collations during dump)
  - Various functions in pg_dump.c that reference collations in other objects

## Notes and Other Information
- This structure is specifically used within the pg_dump utility context for backup and restore operations
- The structure is typedef'd as  for easier usage throughout the codebase
- Collations are crucial for proper text processing in international applications, affecting ORDER BY clauses, comparisons, and text indexing
- The  field ensures that collations are only applied to compatible character encodings
- Collations can be database-specific or template-based, and this structure helps preserve that distinction during dump/restore
- The  field preserves ownership information necessary for proper access control during database restoration
- Collations affect the behavior of string operators, pattern matching, and text indexing operations
- Part of PostgreSQL's internationalization and localization support system