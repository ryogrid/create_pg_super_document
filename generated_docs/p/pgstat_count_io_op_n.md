# pgstat_count_io_op_n

## Location
src/backend/utils/activity/pgstat_io.c: 83 - 99

## Overview
Records multiple occurrences of a specific IO operation in PostgreSQL's statistics system by adding a specified count to the pending IO statistics.

## Definition
void pgstat_count_io_op_n(IOObject io_object, IOContext io_context, IOOp io_op, uint32 cnt)

## Detailed Description
This function is the core implementation for tracking IO operation counts in PostgreSQL's statistics subsystem. It performs comprehensive validation of the input parameters and then increments the appropriate counter in the pending IO statistics structure.

The function includes several important safety checks:
- Validates that all enum parameters are within their valid ranges
- Ensures that the current backend type is configured to track the specified combination of IO object, context, and operation
- Updates the global pending statistics structure and marks that IO statistics are available

The function operates on the PendingIOStats global structure, which accumulates statistics until they are flushed to the statistics collector. This design allows for efficient batching of statistics updates.

## Parameters / Member Variables
- `io_object`: The IOObject enum value specifying the type of object involved in the IO operation
- `io_context`: The IOContext enum value indicating the context in which the IO operation occurred  
- `io_op`: The IOOp enum value identifying the specific type of IO operation being counted
- `cnt`: The number of operations to add to the statistics (uint32 value)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_tracks_io_op
  - IOObject
  - IOContext
  - IOOp
  - IOOBJECT_NUM_TYPES
  - IOCONTEXT_NUM_TYPES
  - IOOP_NUM_TYPES
  - instr_time
- Called from (representative examples):
  - pgstat_count_io_op
  - pgstat_count_io_op_time

## Notes and Other Information
- Uses Assert macros for parameter validation, meaning checks are only active in debug builds
- Updates the global PendingIOStats structure which is later flushed to the statistics collector
- Sets have_iostats to true to indicate that IO statistics are pending
- The MyBackendType global variable is used to determine if the operation should be tracked
- Part of PostgreSQL's comprehensive IO monitoring system for performance analysis