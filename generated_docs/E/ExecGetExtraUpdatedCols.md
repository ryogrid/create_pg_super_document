# ExecGetExtraUpdatedCols

## Location
[src/backend/executor/execUtils.c:1309-1323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1309-L1323)

## Overview
Returns a bitmap representing generated columns that need to be updated as a consequence of an UPDATE operation on other columns.

## Definition


## Detailed Description
This function retrieves the bitmap of generated columns that must be recalculated and updated when an UPDATE operation modifies columns that those generated columns depend on. Generated columns are columns whose values are automatically computed based on expressions involving other columns in the same row.

The function implements lazy initialization - it only computes the generated column information when first requested by calling ExecInitStoredGenerated() if the information hasn't been prepared yet. This optimization avoids unnecessary computation for tables that don't have generated columns or for operations that don't require this information.

The "extra updated columns" represent columns that aren't explicitly mentioned in the UPDATE statement but need to be updated because they are generated columns that depend on columns that are being updated.

## Parameters / Member Variables
- : ResultRelInfo structure for the target relation containing generated column information
- : Executor state containing execution context information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitStoredGenerated](ExecInitStoredGenerated.md): Initializes generated column expressions and computes dependency information
  - CMD_UPDATE: Command type constant indicating UPDATE operation context
- Called from (representative examples):
  - [index_unchanged_by_update](../i/index_unchanged_by_update.md): To determine if indexes need updating due to generated column changes
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md): To compute the complete set of updated columns including generated ones

## Notes and Other Information
- Uses lazy initialization to avoid computing generated column info until needed
- The ri_GeneratedExprsU field being NULL indicates that generated column information hasn't been initialized yet
- Generated columns are automatically maintained by the system and don't require explicit user specification in UPDATE statements
- Critical for maintaining data consistency in tables with generated columns
- The returned bitmap complements the explicitly updated columns to provide a complete picture of all columns that will change
- Essential for index maintenance decisions - indexes on generated columns may need updates even when those columns aren't explicitly mentioned in the UPDATE statement