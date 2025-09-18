# ExecInitSampleScan

## Location
src/backend/executor/nodeSamplescan.c: 93 - 178

## Overview
ExecInitSampleScan initializes a sample scan executor node, setting up all necessary state, expressions, and table sampling infrastructure required for executing TABLESAMPLE operations.

## Definition


## Detailed Description
ExecInitSampleScan is the initialization function for sample scan executor nodes in PostgreSQL. It performs comprehensive setup including creating the SampleScanState structure, opening the scan relation, initializing expression contexts and projections, setting up table sampling parameters, and configuring the specific table sampling method handler. The function handles both cases where a REPEATABLE clause is specified and where a random seed needs to be generated. It defers the actual BeginSampleScan call until later when parameters can be properly evaluated. The initialization follows PostgreSQL's standard executor node pattern while adding sample-specific setup like TSM routine initialization.

## Parameters / Member Variables
- : Pointer to the SampleScan plan node containing the sampling specification and target relation
- : Pointer to the execution state containing transaction context and execution parameters  
- : Execution flags controlling initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - ExecAssignExprContext
  - ExecOpenScanRelation
  - ExecInitScanTupleSlot
  - ExecInitResultTypeTL
  - ExecAssignScanProjectionInfo
  - ExecInitQual
  - ExecInitExprList
  - ExecInitExpr
  - pg_prng_uint32
  - GetTsmRoutine
  - outerPlan/innerPlan (macros)
  - table_slot_callbacks
- Called from (representative examples):
  - ExecInitNode

## Notes and Other Information
- Returns a fully initialized SampleScanState ready for execution
- Asserts that sample scans have no child plans (outerPlan/innerPlan must be NULL)
- Generates a random seed automatically if no REPEATABLE clause is specified
- Sets up the table sampling method (TSM) routine but defers BeginSampleScan until parameters are evaluable
- The begun flag is initialized to false to trigger proper initialization on first tuple request
- Handles all standard executor node initialization including expression contexts, projections, and result types
- Part of the executor node interface that enables TABLESAMPLE clauses to work in SQL queries