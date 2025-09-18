_tocEntryRestorePass

## Overview
This function determines which restore pass a table of contents entry should be processed in, ensuring proper ordering for ACLs, event triggers, and their associated comments during PostgreSQL restoration.

## Definition


## Detailed Description
The  function classifies TOC entries into different restore passes to ensure dependencies are satisfied during restoration. PostgreSQL's restore process uses multiple passes to handle objects that must be created in a specific order due to dependency constraints.

The function implements a simple classification scheme: ACL-related entries (including the obsolete "ACL LANGUAGE" from PostgreSQL 7.4) go into RESTORE_PASS_ACL, event triggers and materialized view data require RESTORE_PASS_POST_ACL, and comments on event triggers must also use RESTORE_PASS_POST_ACL to maintain proper ordering relative to their parent objects. All other entries use the main restore pass.

This ordering is critical because ACLs depend on the existence of their target objects, event triggers have special timing requirements, and comments must be applied after their target objects exist but in the same pass to maintain consistency.

## Parameters / Member Variables
- : Table of contents entry to be classified for restore pass assignment

## Dependencies
- Functions called/Symbols referenced:
  - strcmp/strncmp (C standard library string comparison functions)
  - [RestorePass](../R/RestorePass.md) (enum type defining restore pass constants)
  - RESTORE_PASS_ACL, RESTORE_PASS_POST_ACL, RESTORE_PASS_MAIN (enum values)
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (main restoration orchestration function)
  - [restore_toc_entries_prefork](../r/restore_toc_entries_prefork.md) (parallel restoration setup)
  - [move_to_ready_heap](../m/move_to_ready_heap.md) (dependency resolution for parallel processing)
  - [reduce_dependencies](../r/reduce_dependencies.md) (dependency management during restoration)

## Notes and Other Information
- Returns one of three RestorePass enum values: MAIN, ACL, or POST_ACL
- Handles legacy "ACL LANGUAGE" entries that were only emitted in PostgreSQL 7.4
- Special handling for event trigger comments ensures they're processed with their parent objects
- The multi-pass approach prevents dependency violations during restoration
- Critical for maintaining referential integrity and proper object creation order
- Used extensively in both single-threaded and parallel restoration modes
- The pass assignment affects when objects are created relative to other database objects