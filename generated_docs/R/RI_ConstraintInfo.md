# RI_ConstraintInfo

## Location
src/backend/utils/adt/ri_triggers.c: 100 - 125

## Overview
RI_ConstraintInfo is a structure that stores information extracted from a foreign key constraint entry in pg_constraint and is cached in ri_constraint_cache for efficient access during referential integrity operations.

## Definition


## Detailed Description
RI_ConstraintInfo serves as a cached representation of foreign key constraint metadata to optimize referential integrity checking operations. This structure contains all necessary information about a foreign key relationship, including the participating tables, column mappings, constraint actions, and operator information needed for equality comparisons. The structure supports constraint inheritance through the constraint_root_id field and maintains hash values for efficient cache lookups.

## Parameters / Member Variables
- : OID of the pg_constraint entry, used as the hash key for cache lookup
- : Boolean flag indicating whether the constraint info was successfully initialized
- : OID of the topmost ancestor constraint in inheritance hierarchies
- : Pre-computed hash value of constraint_id for cache efficiency
- : Pre-computed hash value of constraint_root_id for cache efficiency
- : Name of the foreign key constraint
- : OID of the referenced (primary key) relation
- : OID of the referencing (foreign key) relation
- : Character code for the ON UPDATE action (e.g., 'a' for NO ACTION, 'c' for CASCADE)
- : Character code for the ON DELETE action (e.g., 'a' for NO ACTION, 'c' for CASCADE)
- : Number of columns referenced in ON DELETE SET clause
- : Array of attribute numbers for columns to set on delete
- : Foreign key match type (e.g., 'f' for FULL, 'p' for PARTIAL, 's' for SIMPLE)
- : Number of key columns in the constraint
- : Array of attribute numbers for referenced columns
- : Array of attribute numbers for referencing columns
- : Array of equality operators for primary key to foreign key comparisons
- : Array of equality operators for primary key to primary key comparisons
- : Array of equality operators for foreign key to foreign key comparisons
- : Linked list node for maintaining list of valid cache entries

## Dependencies
- Functions called/Symbols referenced:
  - [NameData](../N/NameData.md)
  - RI_MAX_NUMKEYS
  - [dlist_node](../d/dlist_node.md)
- Called from (representative examples):
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ri_LoadConstraintInfo](../r/ri_LoadConstraintInfo.md)
  - [ri_PerformCheck](../r/ri_PerformCheck.md)
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - [RI_FKey_cascade_del](RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](RI_FKey_cascade_upd.md)

## Notes and Other Information
This structure is central to PostgreSQL's referential integrity system and is heavily used during trigger execution for foreign key constraint checking. The caching mechanism improves performance by avoiding repeated lookups of constraint metadata from the system catalogs. The structure supports both simple and complex foreign key relationships, including those with multiple columns and various constraint actions.