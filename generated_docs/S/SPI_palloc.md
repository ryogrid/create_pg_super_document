# SPI_palloc

## Location
src/backend/executor/spi.c: 1338 - 1346

## Overview
Allocates memory in the SPI upper execution context, providing a memory allocation interface for SPI-connected procedures.

## Definition
void *SPI_palloc(Size size)

## Detailed Description
SPI_palloc is a memory allocation function specifically designed for use within the Server Programming Interface (SPI) framework. It allocates memory in the saved memory context of the current SPI connection (_SPI_current->savedcxt), ensuring that the allocated memory persists beyond the lifetime of individual SPI operations. This function acts as a wrapper around PostgreSQL's MemoryContextAlloc function, providing SPI-specific error checking and context management.

The function ensures that memory is allocated in the appropriate context for SPI operations, making it suitable for allocating memory that needs to survive across multiple SPI calls within the same connection session.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - elog (for error reporting)
- Called from (representative examples):
  - [_SPI_strdup](_SPI_strdup.md)
  - Various SPI client functions

## Notes and Other Information
- Must be called while connected to SPI (i.e., after SPI_connect() and before SPI_finish())
- Raises an ERROR if called while not connected to SPI
- Memory allocated by this function should be freed using SPI_pfree() when no longer needed
- The allocated memory persists in the SPI upper execution context, not in the current transaction's memory context
- Part of PostgreSQL's SPI memory management system, designed to provide controlled memory allocation for stored procedures and functions