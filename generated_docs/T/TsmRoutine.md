# TsmRoutine

## Location
[src/include/access/tsmapi.h:56-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tsmapi.h#L56-L76)

## Overview
TsmRoutine is a structure that encapsulates the callback functions and metadata required by PostgreSQL's tablesample method handlers for implementing custom table sampling techniques.

## Definition
```c
typedef struct TsmRoutine
{
    NodeTag         type;
    List           *parameterTypes;
    bool            repeatable_across_queries;
    bool            repeatable_across_scans;
    SampleScanGetSampleSize_function SampleScanGetSampleSize;
    InitSampleScan_function InitSampleScan;
    BeginSampleScan_function BeginSampleScan;
    NextSampleBlock_function NextSampleBlock;
    NextSampleTuple_function NextSampleTuple;
    EndSampleScan_function EndSampleScan;
} TsmRoutine;
```

## Detailed Description
The TsmRoutine structure is the central interface for PostgreSQL's tablesample method API. It serves as a contract between the PostgreSQL core engine and custom tablesample method implementations, defining the callback functions that must be provided by a tablesample handler.

This structure is returned by a tablesample method's handler function and provides both planning-time and execution-time functionality. The planner uses the SampleScanGetSampleSize function to estimate costs, while the executor uses the various scan functions to perform the actual sampling operation.

The design allows for extensible sampling methods while maintaining a consistent interface. Some function pointers can be NULL if the sampling method doesn't require that specific functionality.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TsmRoutine node
- `parameterTypes`: List of datatype OIDs for arguments accepted by the TABLESAMPLE clause
- `repeatable_across_queries`: Whether the method can produce repeatable samples across different queries with the same parameters
- `repeatable_across_scans`: Whether the method can produce repeatable samples within the same query across multiple scans
- `SampleScanGetSampleSize`: Required callback for cost estimation during query planning
- `InitSampleScan`: Optional callback for initializing the sample scan state
- `BeginSampleScan`: Required callback for starting a sample scan with given parameters
- `NextSampleBlock`: Optional callback for block-level sampling (returns next block to sample)
- `NextSampleTuple`: Required callback for tuple-level sampling (returns next tuple offset within a block)
- `EndSampleScan`: Optional callback for cleanup after completing a sample scan

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (from nodes system)
  - [List](../L/List.md) (from nodes/pg_list.h)
- Called from (representative examples):
  - [tsm_bernoulli_handler](../t/tsm_bernoulli_handler.md)
  - [tsm_system_handler](../t/tsm_system_handler.md)
  - [GetTsmRoutine](../G/GetTsmRoutine.md)
  - [ExecInitSampleScan](../E/ExecInitSampleScan.md)
  - [tablesample_init](../t/tablesample_init.md)
  - [set_tablesample_rel_size](../s/set_tablesample_rel_size.md)
  - [cost_samplescan](../c/cost_samplescan.md)

## Notes and Other Information
- The structure should be initialized using makeNode(TsmRoutine) to ensure all fields are properly set to NULL, which is important for forward compatibility as new function pointers may be added in future PostgreSQL versions
- Three callback functions can be NULL: InitSampleScan, NextSampleBlock, and EndSampleScan, providing flexibility in implementation approaches
- The API supports both block-level sampling (via NextSampleBlock) and tuple-level sampling (via NextSampleTuple), allowing for different sampling granularities
- Built-in implementations include Bernoulli sampling (tuple-level) and System sampling (block-level) in src/backend/access/tablesample/
- The callback function signatures are defined as function pointer types in the same header file (tsmapi.h:23-44)