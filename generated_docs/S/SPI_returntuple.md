# SPI_returntuple

## Location
src/backend/executor/spi.c: 1074 - 1105

## Overview
Prepares a tuple for return from a stored procedure by copying it to the upper executor's memory context and ensuring proper type handling for RECORD types.

## Definition


## Detailed Description
SPI_returntuple is an SPI function that converts a HeapTuple into a HeapTupleHeader suitable for returning from a stored procedure or function. The function copies the tuple to the appropriate memory context (the saved context of the current SPI connection) to ensure the tuple remains valid after the SPI context is destroyed. For RECORD types, it also ensures that a proper typmod is assigned to the tuple descriptor if one hasn't been set already.

The function performs several validation checks: it verifies that both the input tuple and tuple descriptor are non-NULL, and that there is an active SPI connection. If any of these conditions fail, it sets the appropriate error code in SPI_result and returns NULL.

## Parameters / Member Variables
- : The HeapTuple to be prepared for return - must be a valid tuple structure
- : The TupleDesc describing the structure of the tuple - must match the tuple's format

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - assign_record_type_typmod
  - heap_copy_tuple_as_datum
  - DatumGetHeapTupleHeader
- Called from (representative examples):
  - Various stored procedure implementations that need to return tuples
  - SPI-based functions that construct return values

## Notes and Other Information
- Sets SPI_result to SPI_ERROR_ARGUMENT if tuple or tupdesc is NULL
- Sets SPI_result to SPI_ERROR_UNCONNECTED if no SPI connection is active
- Automatically assigns a typmod for RECORD types if not already present
- The returned HeapTupleHeader is allocated in the upper executor's memory context
- This function is essential for proper memory management when returning complex types from stored procedures