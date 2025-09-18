# InstrumentOption

## Location
src/include/executor/instrument.h: 66 - 67

## Overview
InstrumentOption is an enumeration that defines flag bits used in the InstrAlloc function's instrument_options bitmask to configure what types of execution statistics and metrics should be collected during query execution.

## Definition


## Detailed Description
InstrumentOption provides a set of bitwise flags that control which types of performance metrics and statistics are collected during PostgreSQL query execution. These flags are combined using bitwise OR operations to create a bitmask that specifies the desired instrumentation level. The enum is designed to allow fine-grained control over what execution statistics are gathered, enabling users to balance performance monitoring needs with execution overhead.

## Parameters / Member Variables
- : Enables timing instrumentation and row count collection for execution nodes
- : Enables buffer usage statistics collection (I/O operations, cache hits/misses)  
- : Enables row count statistics collection independently of timing
- : Enables Write-Ahead Log usage statistics collection
- : Special value (PG_INT32_MAX) that enables all available instrumentation options

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in InstrAlloc function
  - Referenced in explain.c for EXPLAIN command instrumentation options
- Called from (representative examples):
  - InstrAlloc (src/backend/executor/instrument.c:29)
  - explain.c instrumentation setup (src/backend/commands/explain.c:634-641)

## Notes and Other Information
- The enum values use bit shifting to create distinct bit flags that can be combined using bitwise OR operations
- These flags correspond to different performance aspects: execution timing, memory/buffer usage, row processing counts, and WAL generation
- The instrumentation system is used extensively by PostgreSQL's EXPLAIN functionality to provide detailed query execution statistics
- INSTRUMENT_ALL serves as a convenience flag to enable comprehensive instrumentation across all categories
- The flags directly influence which fields are populated in the Instrumentation structure during query execution