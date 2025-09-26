# tablesample_init

## Location
[src/backend/executor/nodeSamplescan.c:218-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L218-L319)

## Overview
Initializes the TABLESAMPLE method by evaluating sampling parameters and calling the BeginSampleScan routine to set up the sampling mechanism.

## Definition
static void tablesample_init(SampleScanState *scanstate)

## Detailed Description
This static function performs the complete initialization of a table sampling scan operation. It evaluates all TABLESAMPLE parameters provided in the query, processes the optional REPEATABLE clause to generate a consistent seed value, and calls the sampling method's BeginSampleScan function. The function also sets up the underlying HeapScanDesc for the table scan, configuring bulk read and page mode settings as determined by the sampling method. The seed generation for REPEATABLE uses a hash of the float8 value to ensure machine-independent results, particularly useful for regression testing.

## Parameters / Member Variables
- `scanstate`: Pointer to the SampleScanState containing all the necessary sampling configuration and state information

## Dependencies
- Functions called/Symbols referenced:
  - [TsmRoutine](../T/TsmRoutine.md) (sampling method interface)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md) (parameter evaluation)
  - [hashfloat8](../h/hashfloat8.md) (seed generation from REPEATABLE parameter)
  - DirectFunctionCall1 (function call wrapper)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (datum conversion)
  - [table_beginscan_sampling](table_beginscan_sampling.md) (heap scan initialization)
  - [table_rescan_set_params](table_rescan_set_params.md) (heap scan reset)
- Called from (representative examples):
  - [SampleNext](../S/SampleNext.md) (in nodeSamplescan.c:48)

## Notes and Other Information
Key initialization steps performed:
- Evaluates all TABLESAMPLE arguments and validates they are not NULL
- Processes REPEATABLE clause if present, converting float8 to uint32 seed via hashing
- Sets default values for use_bulkread and use_pagemode (both true by default)
- Calls the sampling method's BeginSampleScan with evaluated parameters
- Determines whether synchronized scanning can be used based on NextSampleBlock availability
- Creates or resets the HeapScanDesc with appropriate scanning parameters

The REPEATABLE parameter handling is designed to accept both integer and float values at the SQL level, providing compatibility with different database systems while ensuring deterministic results for testing.