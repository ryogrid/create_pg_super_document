# ExecGetUpdatedCols

## Location
src/backend/executor/execUtils.c: 1288 - 1308

## Overview
Returns a bitmap representing the columns being updated for a given result relation, handling column mapping for child relations in partitioned table hierarchies.

## Definition


## Detailed Description
This function retrieves the bitmap of columns that are being updated during an UPDATE operation. It functions similarly to ExecGetInsertedCols but specifically handles updated columns. For regular tables, it returns the updated columns bitmap from the permission info. For child relations in partitioned table hierarchies, it performs attribute mapping to convert the root table's column bitmap to match the child table's column layout.

The function is essential for determining which columns are being modified, which is used for constraint checking, index maintenance optimization, trigger firing decisions, and security enforcement. When dealing with partitioned tables, it ensures that column references are correctly mapped from the parent table's schema to the child table's schema.

## Parameters / Member Variables
- : ResultRelInfo structure for the target relation
- : Executor state containing execution context and memory information

## Dependencies
- Functions called/Symbols referenced:
  - GetResultRTEPermissionInfo: Retrieves permission information for the result relation
  - ExecGetRootToChildMap: Gets the tuple conversion map for partitioned table child relations
  - execute_attr_map_cols: Applies attribute mapping to convert column bitmaps
  - RTEPermissionInfo: Structure containing permission and column information
  - TupleConversionMap: Structure for converting between different tuple schemas
- Called from (representative examples):
  - index_unchanged_by_update: To determine if an index needs updating based on modified columns
  - ExecPartitionCheckEmitError: When validating partition constraints during UPDATE operations
  - ExecConstraints: For enforcing table constraints on updated columns
  - ExecGetAllUpdatedCols: As part of computing the complete set of updated columns including generated columns
  - ExecInitStoredGenerated: For handling generated column dependencies

## Notes and Other Information
- Returns NULL if no permission information is available for the relation
- For non-partitioned tables or root tables, returns the updatedCols bitmap directly from permission info
- For child relations in partitioned tables, applies attribute mapping to ensure column references match the child's schema
- The returned bitmap uses child table's attribute numbers when conversion is performed
- Critical for index maintenance optimization - unchanged indexes can skip updates
- Used by trigger systems to determine which triggers should fire based on column changes
- Essential component of the UPDATE operation's constraint checking and security enforcement mechanisms