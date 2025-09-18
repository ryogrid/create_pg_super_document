# WindowAggStatus

## Location
src/include/nodes/execnodes.h: 2557 - 2558

## Overview
An enumeration that tracks the execution status of window aggregation operations, controlling how window functions are processed and whether results are computed or passed through.

## Definition


## Detailed Description
WindowAggStatus manages the execution state of PostgreSQL's window function processing. Window functions (like ROW_NUMBER(), RANK(), SUM() OVER(), etc.) require special handling because they operate over a window frame within a partition of data. The status controls whether the executor should actively compute window function results or simply pass through existing data, and whether it should continue accumulating tuples for processing. This optimization is particularly important for queries where not all partitions need window function evaluation.

## Parameters / Member Variables
- : Processing is complete - no more tuples to process or return
- : Normal execution mode - actively computing window function results for tuples
- : Optimization mode - bypass window function computation and return tuples as-is
- : Strict pass-through mode - not only bypass computation but also avoid storing new tuples in the buffer during spooling operations

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple enumeration)
- Called from (representative examples):
  - [WindowAggState](WindowAggState.md) (used as status field at execnodes.h:2585)
  - nodeWindowAgg.c:ExecWindowAgg() (status checks and assignments throughout)
  - nodeWindowAgg.c:begin_partition() (transition to WINDOWAGG_RUN at line 2164)
  - nodeWindowAgg.c:ExecInitWindowAgg() (initialization to WINDOWAGG_RUN at line 2652)

## Notes and Other Information
This enum is central to window function optimization strategies. The PASSTHROUGH modes are used when the executor determines that certain partitions don't require window function evaluation, allowing for significant performance improvements. The distinction between PASSTHROUGH and PASSTHROUGH_STRICT relates to memory management - the strict mode prevents unnecessary tuple buffering when the executor knows it won't need to compute window functions for upcoming data. The status transitions are managed carefully to ensure correct results while maximizing performance for complex window queries.