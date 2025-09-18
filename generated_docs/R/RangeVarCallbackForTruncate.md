# RangeVarCallbackForTruncate

## Location
[src/backend/commands/tablecmds.c:17791-17814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17791-L17814)

## Overview
RangeVarCallbackForTruncate is a specialized callback function for RangeVarGetRelidExtended() that validates relations during TRUNCATE command processing.

## Definition
static void RangeVarCallbackForTruncate(const RangeVar *relation, Oid relId, Oid oldRelId, void *arg)

## Detailed Description
This function serves as the validation callback specifically for the TRUNCATE command. It performs comprehensive checks to ensure that the target relation is appropriate for truncation and that the current user has the necessary permissions. The function retrieves the relation's catalog information from the system cache and delegates the actual validation logic to two specialized helper functions: truncate_check_rel for relation type validation and truncate_check_perms for permission verification.

The function follows PostgreSQL's standard pattern for relation callbacks by first checking if the relation exists, then retrieving its metadata from the pg_class system catalog. It uses the system cache for efficient access to relation metadata and properly manages the cache tuple lifecycle.

## Parameters / Member Variables
- : Pointer to RangeVar structure containing the relation name and schema information
- : OID of the relation found during name lookup
- : Previous OID if relation was locked before (used for detecting relation changes)
- : Generic argument pointer (unused in this callback)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](RangeVar.md) (struct representing relation name with optional schema)
  - [truncate_check_rel](../t/truncate_check_rel.md) (function to validate relation type for truncation)
  - [truncate_check_perms](../t/truncate_check_perms.md) (function to check TRUNCATE permissions)
  - Form_pg_class (typedef for pg_class catalog row structure)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup function)
  - HeapTupleIsValid (macro to validate heap tuple)
  - GETSTRUCT (macro to extract structure from heap tuple)
  - [ReleaseSysCache](ReleaseSysCache.md) (system cache cleanup function)
- Called from:
  - [ExecuteTruncate](../E/ExecuteTruncate.md) (main TRUNCATE command implementation)
  - child_dependency_type (during dependency analysis for truncation)

## Notes and Other Information
- This is a static function, only visible within the tablecmds.c file
- Designed specifically for TRUNCATE command validation requirements
- Uses PostgreSQL's system cache mechanism for efficient catalog access
- Delegates actual validation logic to truncate_check_rel and truncate_check_perms helper functions
- Properly manages system cache resources by releasing the tuple after use
- Handles the case where relation lookup succeeds but catalog entry is missing (should not happen under normal circumstances)
- Part of the secure relation access pattern implemented by RangeVarGetRelidExtended