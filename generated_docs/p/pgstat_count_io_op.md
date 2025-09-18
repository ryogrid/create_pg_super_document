# pgstat_count_io_op

## Location
src/backend/utils/activity/pgstat_io.c: 77 - 82

## Overview
A convenience wrapper function that increments the count of a specific IO operation by 1 for PostgreSQL statistics tracking.

## Definition
void pgstat_count_io_op(IOObject io_object, IOContext io_context, IOOp io_op)

## Detailed Description
This function serves as a simple wrapper around pgstat_count_io_op_n() with a fixed increment value of 1. It provides a convenient interface for the common case where only a single IO operation needs to be counted. The function delegates all the actual work to pgstat_count_io_op_n(), which handles the underlying statistics tracking logic.

This is the most commonly used interface for tracking individual IO operations in PostgreSQL, as most code paths need to record single operations rather than batched counts.

## Parameters / Member Variables
- `io_object`: The IOObject enum value specifying the type of object involved in the IO operation (e.g., buffer, relation)
- `io_context`: The IOContext enum value indicating the context in which the IO operation occurred
- `io_op`: The IOOp enum value identifying the specific type of IO operation being counted

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_io_op_n](pgstat_count_io_op_n.md)
  - [IOObject](../I/IOObject.md)
  - [IOContext](../I/IOContext.md)
  - [IOOp](../I/IOOp.md)
- Called from (representative examples):
  - [PinBufferForBlock](../P/PinBufferForBlock.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - GetLocalVictimBuffer

## Notes and Other Information
- This function is purely a convenience wrapper that always increments the count by exactly 1
- For cases where multiple operations need to be counted at once, use pgstat_count_io_op_n() directly
- Part of PostgreSQL's IO statistics subsystem for performance monitoring and analysis
- The actual statistics validation and backend type checking is handled by the underlying pgstat_count_io_op_n() function