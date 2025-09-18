# _tocEntryRequired

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2926 - 3206

## Overview
This function determines whether a table of contents entry should be restored during a PostgreSQL restore operation, applying various filtering rules and returning flags indicating which components (schema, data, or special) should be processed.

## Definition


## Detailed Description
The  function is the central decision-making component for selective restore operations in pg_restore. It evaluates a TOC entry against numerous criteria including restore options, object types, selective filters, and dependency relationships to determine what should be restored.

The function returns a combination of bit flags (REQ_SCHEMA, REQ_DATA, REQ_SPECIAL) or 0 to skip the entry entirely. It handles special cases for database objects, applies exclusion rules for ACLs/comments/security labels, enforces section-based filtering (pre-data/data/post-data), and implements selective restore logic for specific object types.

The decision logic flows through multiple stages: special entry handling, database creation rules, exclusion filters, section validation, ID-based filtering, and finally selective restore rules. For dependent objects like ACLs and comments, it checks whether their parent objects are being restored.

## Parameters / Member Variables
- : Table of contents entry being evaluated for restoration
- : Current section being processed (pre-data, data, or post-data)
- : Archive handle containing restoration context and options

## Dependencies
- Functions called/Symbols referenced:
  - strcmp/strncmp (C standard library string comparison)
  - _tocEntryIsACL (helper function to identify ACL entries)
  - getTocEntryByDumpId (lookup function for dependency resolution)
  - simple_string_list_member (utility for checking list membership)
  - REQ_SCHEMA, REQ_DATA, REQ_SPECIAL (bit flag constants)
  - RestoreOptions, TocEntry, teSection (struct types)
- Called from (representative examples):
  - ProcessArchiveRestoreOptions (during restore option processing)
  - PrintTOCSummary (when generating restore summaries)

## Notes and Other Information
- Returns combination of REQ_SCHEMA (1), REQ_DATA (2), and REQ_SPECIAL (4) bits
- Special entries (ENCODING, STDSTRINGS, SEARCHPATH) are always marked REQ_SPECIAL
- DATABASE entries are only restored when createDB option is enabled
- Implements complex dependency checking for ACLs, comments, and security labels
- Supports selective restoration by schema, table, index, function, and trigger names
- Handles exclusion lists and various skip options (ACLs, comments, publications, etc.)
- Manages schema-only and data-only restore modes with special cases for sequences and large objects
- Large object handling includes special rules for binary upgrade mode
- The function is critical for implementing pg_restore's flexible selective restore capabilities