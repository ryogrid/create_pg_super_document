# RestoreOptions

## Location
src/bin/pg_dump/pg_backup.h: 162 - 163

## Overview
RestoreOptions is a structure that contains configuration options and parameters used during PostgreSQL database restoration operations via pg_restore.

## Definition


## Detailed Description
RestoreOptions is a comprehensive configuration structure that controls all aspects of PostgreSQL database restoration. It encompasses database creation settings, object ownership handling, selective restoration filters, output formatting options, and transaction management parameters. This structure is primarily used by pg_restore to determine how archived database dumps should be restored.

## Parameters / Member Variables
- : Controls whether to issue commands to create the target database
- : When set, doesn't attempt to match original object ownership
- : Prevents issuing table access method related commands
- : Prevents issuing tablespace-related commands during restore
- : Disables triggers during data-only restore operations
- : Uses SET SESSION AUTHORIZATION instead of OWNER TO commands
- : Username to use as superuser during restoration
- : Role to issue SET ROLE command for
- : Controls schema dropping behavior
- : Disables dollar quoting in generated SQL
- : Controls INSERT vs COPY format (0 = COPY, otherwise rows per INSERT)
- : Enables column-specific INSERT statements
- : Adds IF EXISTS clauses to DROP statements
- : Skips comment restoration
- : Skips publication entries during restore
- : Skips security label entries
- : Skips subscription entries
- : Enforces strict name matching
- : Target filename for restoration
- : Restricts restoration to data only
- : Restricts restoration to schema only
- : Bitmask of sections to dump/restore
- : Controls verbosity level of output
- : Skips ACL (access control list) restoration
- : Timeout for lock acquisition
- : Includes all database objects
- : Enables table of contents summary
- : File containing table of contents information
- : Archive format specification
- : Human-readable format name
- : Selection flags for data types
- : Selection flags for indexes
- : Selection flags for functions
- : Selection flags for triggers
- : Selection flags for tables
- : List of specific index names to include
- : List of specific function names to include
- : List of specific schema names to include
- : List of schema names to exclude
- : List of specific trigger names to include
- : List of specific table names to include
- : Flag indicating whether to use database connection
- : Connection parameters for database access
- : Skip data restoration for failed tables
- : Exit immediately on encountering errors
- : Compression specification for archived data
- : Suppresses WARNING entries to stderr
- : Restore all TOC entries in a single transaction
- : Number of TOC entries to restore per transaction (if > 0)
- : Boolean array indicating which dump IDs to emit
- : Enables row-level security during restore
- : Dump sequence data even in schema-only mode
- : Binary upgrade mode flag
- : Restriction key for selective restoration

## Dependencies
- Functions called/Symbols referenced:
  - ConnParams (struct)
  - SimpleStringList (struct)
  - pg_compress_specification (struct)
- Called from (representative examples):
  - NewRestoreOptions
  - SetArchiveOptions
  - ProcessArchiveRestoreOptions
  - RestoreArchive
  - CloneArchive
  - main (pg_restore.c)

## Notes and Other Information
RestoreOptions serves as the central configuration hub for pg_restore operations, providing fine-grained control over every aspect of database restoration. The structure supports selective restoration through various filter mechanisms, transaction control for performance optimization, and comprehensive error handling options. It is typically initialized by NewRestoreOptions() and populated based on command-line arguments parsed by pg_restore.