# RestorePass

## Location
src/bin/pg_dump/pg_backup_archiver.h: 208 - 209

## Overview
An enumeration that defines the different phases (passes) used during PostgreSQL database restoration to ensure proper ordering of operations, particularly for ACLs and event triggers.

## Definition


## Detailed Description
The RestorePass enumeration is a critical component of PostgreSQL's pg_dump/pg_restore mechanism that implements a multi-pass restoration strategy. The restoration process must handle dependencies carefully, particularly ensuring that Access Control Lists (ACLs) are restored after the objects they protect, and that event triggers and materialized view refreshes are restored after ACLs to prevent interference.

The three-pass approach ensures:
1. **RESTORE_PASS_MAIN**: Handles most database objects (tables, functions, etc.)
2. **RESTORE_PASS_ACL**: Processes ACLs and related permissions after main objects exist
3. **RESTORE_PASS_POST_ACL**: Handles event triggers and materialized view data that should run after ACLs are established

This mechanism addresses the limitation that the dependency sorting alone cannot handle all ordering requirements, particularly the complex interactions between ACLs, event triggers, and materialized view refreshes.

## Parameters / Member Variables
- : The primary restoration pass handling most TOC (Table of Contents) item types including tables, functions, indexes, and other standard database objects
- : Dedicated pass for Access Control Lists, DEFAULT ACLs, and the legacy "ACL LANGUAGE" entries from PostgreSQL 7.4
- : Final pass for event triggers and materialized view data refreshes that must occur after ACL establishment
- : Macro defining the final pass (currently equivalent to RESTORE_PASS_POST_ACL)

## Dependencies
- Functions called/Symbols referenced:
  - Used by  function to determine which pass a TOC entry belongs to
  - Referenced in  for parallel restoration scheduling
  - Used in  struct's  member during parallel restore operations

- Called from (representative examples):
  -  in src/bin/pg_dump/pg_backup_archiver.c:3207
  - Restoration scheduling logic in parallel restore functions
  - Archive handle initialization and processing

## Notes and Other Information
- The enum is defined in src/bin/pg_dump/pg_backup_archiver.h:201-208
- This mechanism is intended to be superseded by proper dependency tracking for ACLs, but will remain necessary for compatibility with older dump files
- Comments for event triggers are specially handled to be restored in the same pass as the event triggers themselves (RESTORE_PASS_POST_ACL)
- The design acknowledges that while dependency sorting handles most ordering requirements, special cases like ACL dependencies require explicit multi-pass handling
- Used extensively in both single-threaded and parallel restoration modes