# MultiExecBitmapOr

## Location
src/backend/executor/nodeBitmapOr.c: 111 - 195

## Overview
MultiExecBitmapOr executes a BitmapOr node by combining the bitmap results from all child subplans using logical OR operations to produce a unified TID bitmap.

## Definition


## Detailed Description
MultiExecBitmapOr is the core execution function for BitmapOr nodes, implementing the actual bitmap OR logic. The function iterates through all child subplans, executes each one to obtain a TID (Tuple Identifier) bitmap, and combines these bitmaps using logical OR operations to create a unified result bitmap.

The function includes an important optimization for BitmapIndexScan children: instead of performing explicit tbm_union operations, it passes the current result bitmap directly to BitmapIndexScan nodes, allowing them to OR their results directly into the target bitmap. This reduces memory allocation and copying overhead.

For non-BitmapIndexScan children, the function uses the standard approach of executing each subplan independently and then using tbm_union to combine the results. The function handles instrumentation for performance monitoring and includes error checking to ensure all subplans return valid TID bitmaps.

## Parameters / Member Variables
- : Pointer to the BitmapOrState containing the execution context and child plan states

## Dependencies
- Functions called/Symbols referenced:
  - InstrStartNode/InstrStopNode (performance instrumentation)
  - [tbm_create](../t/tbm_create.md) (creates initial TID bitmap)
  - [MultiExecProcNode](MultiExecProcNode.md) (executes child subplans)
  - tbm_union (combines bitmaps using OR operation)
  - [tbm_free](../t/tbm_free.md) (deallocates temporary bitmaps)
  - IsA (type checking for optimization)
  - elog (error reporting)
  - work_mem (global variable for memory limits)

- Called from (representative examples):
  - [MultiExecProcNode](MultiExecProcNode.md) (part of the multi-execution dispatch system)

## Notes and Other Information
- Uses work_mem to determine initial bitmap size allocation
- Supports shared bitmap allocation through query-level DSA (Dynamic Shared Area)
- Implements special optimization for BitmapIndexScan children to avoid extra bitmap copies
- Requires at least one input subplan (errors if zero inputs provided)
- Returns TIDBitmap rather than tuple slots, following the multi-execution interface pattern
- Provides manual instrumentation support since it doesn't use standard execution framework
- All child bitmaps except the first are freed after union operation to manage memory