# heap_truncate_check_FKs

## Location
src/backend/catalog/heap.c: 3154 - 3248

## Overview
heap_truncate_check_FKs validates that relations to be truncated do not have foreign key constraints that would be violated by the truncation operation.

## Definition
void heap_truncate_check_FKs(List *relations, bool tempTables)

## Detailed Description
This function enforces foreign key constraint validation before allowing truncation operations to proceed. It scans the list of relations to be truncated and identifies any foreign key dependencies that would prevent truncation. The function allows self-referential foreign keys but disallows foreign keys from external tables since TRUNCATE is designed to avoid scanning individual rows.

The function operates in multiple phases: first building a list of relation OIDs that have triggers or are partitioned tables (since these can have foreign keys), then performing a fast check using heap_truncate_find_FKs. If violations are found, it performs detailed scans to identify specific constraint violations and generates appropriate error messages based on whether temporary tables are involved.

## Parameters / Member Variables
- : List of Relation structures to be truncated
- : Boolean flag indicating if these are temporary tables (affects error message selection)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - RelationGetRelid
  - lappend_oid
  - [heap_truncate_find_FKs](heap_truncate_find_FKs.md)
  - lfirst_oid
  - list_make1_oid
  - [list_member_oid](../l/list_member_oid.md)
  - [get_rel_name](../g/get_rel_name.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
- Called from (representative examples):
  - [heap_truncate](heap_truncate.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
- Shared by both transaction-safe and non-transaction-safe truncate implementations
- Relations without triggers are skipped since they cannot have foreign keys (except partitioned tables)
- Self-referential foreign keys are allowed since they don't prevent truncation
- Provides different error messages for temporary vs. permanent tables
- Performance optimization: fast path returns early if no relations have triggers
- Uses two-phase scanning: bulk check first, then detailed analysis for error reporting
- The function assumes caller already holds appropriate locks on the relations