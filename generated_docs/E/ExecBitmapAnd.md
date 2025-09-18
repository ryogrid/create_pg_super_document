# ExecBitmapAnd

## Location
src/backend/executor/nodeBitmapAnd.c: 42 - 54

## Overview
ExecBitmapAnd is a stub function that exists for pro forma compliance with the PostgreSQL executor node interface but is not intended to be called during normal execution.

## Definition


## Detailed Description
ExecBitmapAnd serves as a placeholder function that implements the standard ExecProcNode interface for BitmapAnd nodes, but it is not actually used during query execution. Instead of processing tuples like other executor nodes, BitmapAnd nodes use the MultiExecProcNode interface through MultiExecBitmapAnd to produce bitmap results.

The function immediately throws an error if called, indicating that BitmapAnd nodes do not support the conventional ExecProcNode call convention. This design reflects the fact that bitmap scan nodes operate differently from regular tuple-producing nodes - they generate bitmaps of qualifying tuple identifiers rather than streams of tuples.

## Parameters / Member Variables
- : PlanState pointer to the BitmapAnd node state (unused, as function immediately errors)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from (representative examples):
  - ExecInitBitmapAnd (sets this as the ExecProcNode function pointer)

## Notes and Other Information
- This function exists only to satisfy the executor node interface requirements
- The actual execution logic for BitmapAnd nodes is implemented in MultiExecBitmapAnd
- BitmapAnd nodes are part of PostgreSQL's bitmap index scan optimization
- The error message clearly indicates the correct execution path should use MultiExecProcNode instead
- Located in src/backend/executor/nodeBitmapAnd.c:42-54