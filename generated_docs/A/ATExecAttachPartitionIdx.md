# ATExecAttachPartitionIdx

## Location
src/backend/commands/tablecmds.c: 19849 - 20003

## Overview
Implements the ALTER INDEX ATTACH PARTITION command to attach a partition index to a partitioned parent index, establishing the parent-child relationship in the partitioning hierarchy.

## Definition


## Detailed Description
This function handles the complex process of attaching an index on a partition table to its corresponding parent partitioned index. The operation involves multiple validation steps and relationship establishment:

1. **Lock Management**: Carefully acquires locks on the parent index, partition index, and their respective tables to prevent deadlocks and race conditions
2. **Index Resolution**: Uses RangeVarCallbackForAttachIndex to resolve the partition index name and validate it's actually an index
3. **Validation Checks**: Performs comprehensive validation including:
   - Ensuring no duplicate attachments exist
   - Verifying the partition table is actually a partition of the parent table
   - Comparing index definitions for compatibility (columns, collations, operator families)
   - Checking constraint consistency between parent and partition
4. **Relationship Establishment**: Sets up the parent-child relationship for both indexes and any associated constraints
5. **Final Validation**: Validates the complete partitioned index structure

The function is designed to be idempotent - if the attachment already exists in the correct state, it silently succeeds.

## Parameters / Member Variables
- : Work queue for deferred operations (currently unused in this function)
- : The parent partitioned index relation to which the partition index will be attached
- : RangeVar specifying the name of the partition index to be attached

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended
  - RangeVarCallbackForAttachIndex
  - relation_open
  - relation_close
  - get_partition_parent
  - refuseDupeIndexAttach
  - RelationGetPartitionDesc
  - BuildIndexInfo
  - CompareIndexInfo
  - build_attrmap_by_name
  - free_attrmap
  - get_relation_idx_constraint_oid
  - IndexSetParentIndex
  - ConstraintSetParentConstraint
  - validatePartitionedIndex
- Called from (representative examples):
  - ATExecCmd (as part of ALTER TABLE command processing)

## Notes and Other Information
- This function implements the core logic for ALTER INDEX ATTACH PARTITION DDL command
- Lock ordering is critical: table locks are acquired before index locks to prevent deadlocks
- The function performs extensive validation to ensure index compatibility, including column mapping through attribute maps
- Constraint inheritance is handled automatically when both parent and partition have associated constraints
- AccessExclusiveLock is used on the partition index to prevent concurrent modifications during attachment
- Error messages are detailed and provide specific information about validation failures
- The function maintains transactional semantics - all changes are committed together or rolled back on error