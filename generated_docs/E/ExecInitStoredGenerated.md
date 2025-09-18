# ExecInitStoredGenerated

## Location
src/backend/executor/nodeModifyTable.c: 373 - 472

## Overview
Initializes the computation of stored generated columns for a tuple by preparing the necessary expression trees and tracking which generated columns need to be computed based on the command type.

## Definition
```c
void ExecInitStoredGenerated(ResultRelInfo *resultRelInfo, EState *estate, CmdType cmdtype)
```

## Detailed Description
ExecInitStoredGenerated sets up the infrastructure needed to compute stored generated columns during DML operations. The function analyzes the target relation's tuple descriptor to identify generated columns and prepares their expression trees for execution.

Key functionality includes:
- For INSERT operations: Prepares all stored generated column expressions
- For UPDATE operations: Optimizes by only preparing expressions for generated columns that depend on updated columns (unless BEFORE ROW triggers exist)
- Handles both INSERT and UPDATE scenarios for MERGE operations and cross-partition updates
- Allocates expression arrays and tracks the count of generated columns that need computation
- For UPDATEs, marks generated columns in ri_extraUpdatedCols to track additional column updates

The function performs dependency analysis to avoid unnecessary computation when possible, checking if generated column expressions reference any of the columns being updated.

## Parameters / Member Variables
- `resultRelInfo`: Result relation information structure that will store the prepared expressions
- `estate`: Executor state providing query context and memory management
- `cmdtype`: Command type (CMD_INSERT, CMD_UPDATE, etc.) determining which expression arrays to populate

## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - CMD_UPDATE
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md)
  - ATTRIBUTE_GENERATED_STORED
  - [build_column_default](../b/build_column_default.md)
  - [pull_varattnos](../p/pull_varattnos.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [ExecPrepareExpr](ExecPrepareExpr.md)
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [ExecGetExtraUpdatedCols](ExecGetExtraUpdatedCols.md)
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md)

## Notes and Other Information
- This function is part of PostgreSQL's generated columns feature implementation
- The function handles memory context management by allocating structures in the per-query context for query lifetime persistence
- Optimization for UPDATE operations skips computation of generated columns that don't depend on updated columns, unless BEFORE ROW UPDATE triggers are present
- The function supports both ri_GeneratedExprsI (for INSERT) and ri_GeneratedExprsU (for UPDATE) to handle MERGE operations efficiently
- Error handling includes specific messages when generation expressions are missing for declared generated columns
- The function is designed to be called only once per command type per result relation (enforced by assertions)