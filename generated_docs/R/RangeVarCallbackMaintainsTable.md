# RangeVarCallbackMaintainsTable

## Location
src/backend/commands/tablecmds.c: 17755 - 17790

## Overview
RangeVarCallbackMaintainsTable is a callback function for RangeVarGetRelidExtended() that validates relation types and checks MAINTAIN permissions for maintenance operations.

## Definition
void RangeVarCallbackMaintainsTable(const RangeVar *relation, Oid relId, Oid oldRelId, void *arg)

## Detailed Description
This function serves as a specialized callback for relation lookup operations that require maintenance-level permissions. It implements a two-stage validation process: first checking that the target relation is an appropriate type for maintenance operations (plain table, partitioned table, materialized view, or TOAST table), then verifying that the current user has MAINTAIN privileges on the relation.

The function is designed to handle race conditions where a relation might be dropped between the name lookup and the callback execution. It gracefully handles such cases by checking if the relation still exists before proceeding with validation. The permission checking follows PostgreSQL's ACL (Access Control List) system, requiring either ownership or explicit MAINTAIN grant.

## Parameters / Member Variables
- : Pointer to RangeVar structure containing the relation name and schema information
- : OID of the relation found during name lookup
- : Previous OID if relation was locked before (used for detecting relation changes)
- : Generic argument pointer (unused in this callback)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (struct representing relation name with optional schema)
  - AclResult (enum for access control check results)
  - get_rel_relkind (function to get relation kind)
  - RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE (relation type constants)
  - pg_class_aclcheck (function to check access permissions)
  - ACL_MAINTAIN (permission constant for maintenance operations)
  - aclcheck_error (function to generate permission error messages)
  - get_relkind_objtype (function to get object type string from relation kind)
- Called from:
  - cluster (CLUSTER command implementation)
  - ReindexTable (REINDEX TABLE command implementation)
  - ExecRefreshMatView (REFRESH MATERIALIZED VIEW command implementation)

## Notes and Other Information
- Designed specifically for maintenance operations requiring MAINTAIN privilege
- Handles race conditions by checking relation existence after name lookup
- Rejects inappropriate relation types with descriptive error messages
- Used by multiple maintenance commands (CLUSTER, REINDEX TABLE, REFRESH MATERIALIZED VIEW)
- Part of the RangeVarGetRelidExtended callback system for secure relation access
- Implements the principle of least privilege by requiring specific MAINTAIN permission rather than broader privileges