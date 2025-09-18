# ExecGetInsertedCols

## Location
src/backend/executor/execUtils.c: 1267 - 1287

## Overview
Returns a bitmap representing the columns being inserted for a given result relation, handling column mapping for child relations in partitioned table hierarchies.

## Definition


## Detailed Description
This function retrieves the bitmap of columns that are being inserted into a relation during an INSERT operation. For regular tables, it simply returns the inserted columns bitmap from the permission info. For child relations in partitioned table hierarchies, it performs attribute mapping to convert the root table's column bitmap to match the child table's column layout.

The function handles the complexity of partitioned tables where child tables may have different column orders or additional columns compared to the root table. When a conversion map is needed, it uses the attribute mapping to translate column references from the root table's schema to the child table's schema.

## Parameters / Member Variables
- : ResultRelInfo structure for the target relation
- : Executor state containing execution context and memory information

## Dependencies
- Functions called/Symbols referenced:
  - [GetResultRTEPermissionInfo](../G/GetResultRTEPermissionInfo.md): Retrieves permission information for the result relation
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md): Gets the tuple conversion map for partitioned table child relations
  - [execute_attr_map_cols](../e/execute_attr_map_cols.md): Applies attribute mapping to convert column bitmaps
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md): Structure containing permission and column information
  - TupleConversionMap: Structure for converting between different tuple schemas
- Called from (representative examples):
  - [ExecPartitionCheckEmitError](ExecPartitionCheckEmitError.md): When validating partition constraints during INSERT operations
  - [ExecConstraints](ExecConstraints.md): For enforcing table constraints on inserted columns
  - [ExecWithCheckOptions](ExecWithCheckOptions.md): When processing WITH CHECK OPTION clauses on views

## Notes and Other Information
- Returns NULL if no permission information is available for the relation
- For non-partitioned tables or root tables, returns the insertedCols bitmap directly from permission info
- For child relations in partitioned tables, applies attribute mapping to ensure column references match the child's schema
- The returned bitmap uses child table's attribute numbers when conversion is performed
- This function is essential for constraint checking and security enforcement during INSERT operations in partitioned table environments