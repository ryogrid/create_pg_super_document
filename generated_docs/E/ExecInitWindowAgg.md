# ExecInitWindowAgg

## Location
src/backend/executor/nodeWindowAgg.c: 2374 - 2680

## Overview
Initialization function for WindowAgg execution nodes that creates the runtime state structure, sets up window function processing infrastructure, and prepares all necessary components for window aggregation execution.

## Definition


## Detailed Description
ExecInitWindowAgg is the comprehensive initialization function for window aggregation nodes in PostgreSQL's executor. It transforms the planner-generated WindowAgg plan node into a fully operational WindowAggState execution state, setting up all the complex infrastructure needed for window function processing.

The function performs several major initialization tasks:

**State Structure Setup**: Creates the WindowAggState node and links it to the execution engine, setting ExecWindowAgg as the execution function.

**Memory Context Management**: Establishes multiple specialized memory contexts:
- General expression contexts for per-input and per-output tuple processing
- Partition context for partition-local storage that persists across rows in a partition  
- Aggregate context for window aggregate transition values

**Tuple Slot Initialization**: Creates various tuple slots for different purposes:
- Scan slots for input/output processing
- Temporary slots for intermediate computations
- Conditional frame head/tail slots for RANGE/GROUPS mode boundary tracking

**Expression and Qualification Setup**: Initializes filter qualifications and run conditions that enable performance optimizations like pass-through mode.

**Window Function Processing Setup**: Analyzes all window functions to:
- Detect and deduplicate identical window functions
- Set up per-function state including result types and collations
- Distinguish between plain aggregates (winagg=true) and true window functions
- Initialize function call infrastructure and permissions checking

**Aggregate Infrastructure**: For window functions that are actually aggregates, sets up the traditional aggregate processing machinery including WindowObject structures.

**Frame Boundary Support**: Initializes offset expressions and in_range support functions needed for RANGE mode frame boundary calculations.

**Comparison Function Setup**: Prepares tuple comparison functions for PARTITION BY and ORDER BY clauses.

## Parameters / Member Variables
- : WindowAgg plan node containing planner specifications including:
  - Window function list and frame options
  - Partition and ordering column specifications  
  - Frame boundary offset expressions
  - Run condition expressions for optimization
- : Executor state providing global execution context
- : Execution flags (BACKWARD and MARK not supported for WindowAgg)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (state structure creation)
  - ExecAssignExprContext (expression context setup)
  - AllocSetContextCreate (memory context creation)  
  - [ExecInitQual](ExecInitQual.md) (qualification expression initialization)
  - [ExecInitNode](ExecInitNode.md) (child node initialization)
  - ExecCreateScanSlotFromOuterPlan/ExecInitExtraTupleSlot (tuple slot setup)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)/ExecAssignProjectionInfo (projection setup)
  - execTuplesMatchPrepare (tuple comparison function preparation)
  - [object_aclcheck](../o/object_aclcheck.md)/aclcheck_error (permission checking)
  - [get_typlenbyval](../g/get_typlenbyval.md) (type information retrieval)
  - initialize_peragg (aggregate state setup)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)/fmgr_info_set_expr (function call setup)
  - [ExecInitExpr](ExecInitExpr.md) (expression initialization)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (general executor node initialization dispatch)

## Notes and Other Information
- Returns fully initialized WindowAggState ready for execution via ExecWindowAgg
- Handles deduplication of identical window functions for performance optimization
- Only top-level WindowAgg nodes may have filter qualifications
- Sets up pass-through mode capabilities when run conditions are present
- Performs comprehensive permission checking for all window functions
- Creates specialized WindowObject structures that provide the interface between window functions and the execution engine
- Frame head/tail slots are created conditionally based on frame options to minimize memory usage
- Uses query-lifetime memory contexts for long-lived data like offset values and function call information
- Critical initialization path that must correctly set up all state for complex multi-function window processing