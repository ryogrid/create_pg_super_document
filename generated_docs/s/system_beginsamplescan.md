# system_beginsamplescan

## Location
[src/backend/access/tablesample/system.c:139-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/tablesample/system.c#L139-L177)

## Overview
Examines sampling parameters and configures the SystemSamplerData structure to prepare for executing a SYSTEM table sampling scan.

## Definition
```c
static void system_beginsamplescan(SampleScanState *node,
                                  Datum *params,
                                  int nparams,
                                  uint32 seed)
```

## Detailed Description
This function initializes the sampling parameters for the SYSTEM table sampling method before the actual scan begins. It validates the sampling percentage parameter, calculates a hash cutoff value that determines which blocks will be selected for sampling, and configures various scan optimization settings. The function converts the percentage parameter into a cutoff value that will be used to make probabilistic decisions about block selection. It also sets up buffer access strategies and visibility checking modes to optimize the scanning process based on the sampling percentage.

## Parameters / Member Variables
- `node`: SampleScanState structure containing the sample scan execution state
- `params`: Array of Datum values containing the sampling parameters (expects sampling percentage as first parameter)
- `nparams`: Number of parameters in the params array (should be 1 for SYSTEM method)
- `seed`: Random seed value for reproducible sampling results

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetFloat4](../D/DatumGetFloat4.md) (extracts float4 value from first parameter)
  - isnan (checks for NaN values)
  - ereport/errcode/errmsg (error reporting functions)
  - rint (rounds double to nearest integer)
  - PG_UINT32_MAX (maximum 32-bit unsigned integer constant)
  - InvalidOffsetNumber (invalid offset constant)
- Called from (representative examples):
  - [tsm_system_handler](../t/tsm_system_handler.md) (as function pointer in TsmRoutine)
  - PostgreSQL executor when starting a sample scan

## Notes and Other Information
- Validates that the sampling percentage is between 0 and 100 and not NaN, raising an error otherwise
- Calculates cutoff as a 64-bit value representing the sampling probability scaled to the full uint32 range
- Uses bulkread buffer strategy for sampling percentages >= 1% for better I/O performance
- Always enables pagemode visibility checking since all tuples on selected pages are scanned
- Initializes nextblock to 0 and last tuple offset to InvalidOffsetNumber to start scanning from the beginning
- The seed parameter enables repeatable sampling results when the same seed is used