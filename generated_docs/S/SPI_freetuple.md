# SPI_freetuple

## Location
src/backend/executor/spi.c: 1379 - 1385

## Overview
Frees a HeapTuple structure previously allocated or returned by SPI operations, providing tuple memory management for SPI-connected procedures.

## Definition
void SPI_freetuple(HeapTuple tuple)

## Detailed Description
SPI_freetuple is a memory deallocation function specifically designed for freeing HeapTuple structures within the Server Programming Interface (SPI) framework. This function serves as a wrapper around PostgreSQL's heap_freetuple function, providing a simplified interface for tuple memory management in SPI contexts.

The function eliminates the need for SPI clients to track which memory context a particular tuple was allocated in, as indicated by the comment in the source code. The underlying heap_freetuple function can determine the appropriate context and perform the necessary cleanup operations automatically.

## Parameters / Member Variables
- `tuple`: Pointer to the HeapTuple structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - SPI client functions that need to clean up tuple results
  - Functions processing SPI query results

## Notes and Other Information
- Acts as a thin wrapper around PostgreSQL's heap_freetuple function
- Should be used to free HeapTuple structures returned by SPI functions or created within SPI contexts
- Does not require explicit memory context management as the underlying heap_freetuple handles context determination
- Essential for preventing memory leaks when processing query results in SPI-based stored procedures and functions
- Part of PostgreSQL's SPI memory management system, specifically handling tuple-related memory cleanup
- Attempting to free a NULL tuple pointer is typically handled safely by the underlying implementation
- Complements other SPI memory management functions like SPI_palloc, SPI_repalloc, and SPI_pfree for comprehensive memory management