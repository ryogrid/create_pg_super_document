# SPI_datumTransfer

## Location
src/backend/executor/spi.c: 1361 - 1378

## Overview
Transfers a Datum value to the SPI upper execution context, ensuring the data persists beyond individual SPI operations.

## Definition
Datum SPI_datumTransfer(Datum value, bool typByVal, int typLen)

## Detailed Description
SPI_datumTransfer is a data transfer function designed for use within the Server Programming Interface (SPI) framework. This function copies a Datum value from its current memory context to the saved memory context of the current SPI connection (_SPI_current->savedcxt). This transfer ensures that the data remains accessible and valid throughout the lifetime of the SPI connection, even after the original memory context where the value was created has been destroyed.

The function temporarily switches to the SPI saved memory context, performs the datum transfer using PostgreSQL's datumTransfer function, and then switches back to the original context. This process ensures that any memory allocations required for copying the datum (particularly for pass-by-reference types) occur in the appropriate long-lived context.

## Parameters / Member Variables
- `value`: The Datum value to be transferred
- `typByVal`: Boolean indicating whether the type is passed by value (true) or by reference (false)
- `typLen`: Length specification for the data type (-1 for variable length, -2 for C string, positive value for fixed length)

## Dependencies
- Functions called/Symbols referenced:
  - datumTransfer
  - MemoryContextSwitchTo
  - elog (for error reporting)
- Called from (representative examples):
  - Various SPI functions that need to preserve datum values across context boundaries

## Notes and Other Information
- Must be called while connected to SPI (i.e., after SPI_connect() and before SPI_finish())
- Raises an ERROR if called while not connected to SPI
- For pass-by-value types, simply returns the original value as no copying is needed
- For pass-by-reference types, creates a copy of the data in the SPI saved context
- Essential for preserving query results and other data structures that need to survive beyond individual SPI operations
- The transferred datum becomes the caller's responsibility to manage within the SPI context
- Part of PostgreSQL's SPI data management system, complementing the memory management functions