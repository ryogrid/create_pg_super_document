# DefineIndex

## Location
src/backend/commands/indexcmds.c: 540 - 1791

## Overview
Creates a new index on a specified table relation, handling both regular and concurrent index creation with comprehensive validation, permission checks, and support for partitioned tables.

## Definition
```c
ObjectAddress DefineIndex(Oid tableId, IndexStmt *stmt, Oid indexRelationId, Oid parentIndexId, Oid parentConstraintId, int total_parts, bool is_alter_table, bool check_rights, bool check_not_in_use, bool skip_build, bool quiet)
```

## Detailed Description
DefineIndex is the primary function responsible for creating indexes in PostgreSQL. It manages a complex workflow that includes access method validation, permission checking, attribute processing, constraint handling, and the actual index creation process. The function supports both regular and concurrent index creation modes.

The function manages user identity and security context carefully, especially for pg_dump compatibility - using the table owner userid for most ACL checks while preserving the original userid for predictable ACL checks. This addresses the complexity of running opaque expressions (like function calls) safely during index creation.

Key responsibilities include:
- Validating the access method and its capabilities
- Processing index attributes, including key columns and INCLUDE columns  
- Handling partitioned table constraints and validation
- Managing concurrent vs. non-concurrent build modes
- Creating catalog entries and optionally building the index
- Supporting primary key, unique, and exclusion constraints
- Managing progress reporting for long-running operations

For concurrent builds, the function implements a multi-phase process:
1. Create catalog entries with index marked as not ready for inserts
2. Wait for existing transactions to complete
3. Build the index while allowing concurrent DML
4. Wait again for snapshot consistency
5. Validate the index and mark it as ready

## Parameters / Member Variables
- `tableId`: OID of the table relation on which the index is to be created
- `stmt`: IndexStmt describing the properties of the new index
- `indexRelationId`: Normally InvalidOid, but can specify a preselected OID during bootstrap
- `parentIndexId`: OID of the parent index; InvalidOid if not a child of a partitioned index
- `parentConstraintId`: OID of the parent constraint; InvalidOid if not a constraint child
- `total_parts`: Total number of direct and indirect partitions; -1 if unknown or not partitioned
- `is_alter_table`: Boolean indicating this is due to ALTER rather than CREATE operation
- `check_rights`: Whether to check CREATE rights in namespace and tablespace
- `check_not_in_use`: Whether to check that table is not in use in current session
- `skip_build`: Whether to make catalog entries but skip building index files
- `quiet`: Whether to suppress NOTICE messages for constraints

## Dependencies
- Functions called/Symbols referenced:
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - [CheckPredicate](../C/CheckPredicate.md)  
  - [ChooseIndexName](../C/ChooseIndexName.md)
  - [ChooseIndexColumnNames](../C/ChooseIndexColumnNames.md)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md)
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md)
  - index_create
  - index_concurrently_build
  - [validate_index](../v/validate_index.md)
  - [CreateComments](../C/CreateComments.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [DefineRelation](DefineRelation.md)
  - [ATExecAddIndex](../A/ATExecAddIndex.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)

## Notes and Other Information
- Supports both regular and concurrent index creation modes
- Handles complex partitioned table scenarios with recursive index creation on partitions
- Implements comprehensive validation for unique constraints on partitioned tables
- Manages GUC variables and security context carefully during execution
- Progress reporting is integrated for monitoring long-running operations
- Contains special handling for obsolete RTREE access method (converts to GiST)
- Validates that partition keys are included in unique/exclusion constraints for partitioned tables
- Located in src/backend/commands/indexcmds.c:540-1791