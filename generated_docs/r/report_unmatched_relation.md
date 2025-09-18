# report_unmatched_relation

## Location
src/bin/pg_upgrade/info.c: 211 - 278

## Overview
Reports detailed information about relations that could not be matched between old and new PostgreSQL clusters during upgrade operations, providing contextual descriptions for debugging.

## Definition


## Detailed Description
This static function generates detailed error reports when a relation from one cluster cannot be matched with a corresponding relation in the other cluster during pg_upgrade operations. It builds a comprehensive description of the unmatched relation, including its namespace and name, and provides additional context if the relation is an index or TOAST table by identifying the table it belongs to.

The function performs lookups to find related tables when dealing with indexes (via indtable field) or TOAST tables (via toastheap field), providing human-readable descriptions that help administrators understand what relation failed to match and why. It handles cases where the related table information might also be missing.

## Parameters / Member Variables
- : Pointer to RelInfo structure containing information about the unmatched relation
- : Pointer to DbInfo structure containing database information where the relation exists
- : Boolean flag indicating whether the unmatched relation is from the new cluster (true) or old cluster (false)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - strlen
  - pg_log
  - _ (gettext translation macro)
- Data structures used:
  - RelInfo
  - DbInfo
  - Oid
- Called from (representative examples):
  - gen_db_file_maps (multiple call sites)

## Notes and Other Information
- Static function - only accessible within the same source file (info.c)
- Uses a 1000-character buffer to build descriptive messages about unmatched relations
- Provides special handling for indexes by identifying the table they index via the indtable field
- Provides special handling for TOAST tables by identifying the heap table they belong to via the toastheap field
- Generates different log messages depending on whether the unmatched relation is from the old or new cluster
- Part of pg_upgrade's error reporting and debugging infrastructure
- Helps administrators understand upgrade failures related to schema mismatches between clusters