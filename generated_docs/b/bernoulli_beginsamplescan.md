# bernoulli_beginsamplescan

## Location
src/backend/access/tablesample/bernoulli.c: 136 - 180

## Overview
This function examines the sampling parameters and prepares the Bernoulli sampler for execution by setting up the probability cutoff and scan optimization flags.

## Definition
```c
static void bernoulli_beginsamplescan(SampleScanState *node,
                                      Datum *params,
                                      int nparams,
                                      uint32 seed)
```

## Detailed Description
The `bernoulli_beginsamplescan` function initializes the Bernoulli sampler with the actual sampling parameters provided at execution time. It validates the sampling percentage, calculates a probability cutoff value used for random sampling decisions, and configures scan optimization settings. The cutoff calculation converts the percentage to a 64-bit integer threshold that can be efficiently compared against random numbers during tuple sampling. The function also determines whether to use bulk reading and page-mode visibility checking based on the sampling percentage.

## Parameters / Member Variables
- `node`: SampleScanState structure representing the sample scan execution node
- `params`: Array of Datum values containing sampling parameters (first element is sampling percentage)
- `nparams`: Number of parameters (expected to be 1 for Bernoulli sampling)
- `seed`: Random seed for repeatable sampling

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetFloat4 (extracts float4 sampling percentage from Datum)
  - isnan (validates percentage is not NaN)
  - rint (rounds cutoff calculation to nearest integer)
  - ereport/ERROR (reports parameter validation errors)
  - PG_UINT32_MAX (maximum 32-bit unsigned integer for cutoff calculation)
  - InvalidOffsetNumber (initializes last tuple offset)
- Called from (representative examples):
  - tsm_bernoulli_handler (sets this as BeginSampleScan callback)

## Notes and Other Information
- Validates sampling percentage is between 0 and 100, raising an error otherwise
- Calculates cutoff as (PG_UINT32_MAX + 1) * percent / 100 for precise probability comparison
- Stores the seed for potentially repeatable random number generation
- Initializes lt (last tuple) to InvalidOffsetNumber to indicate no previous tuple
- Enables bulk reading since Bernoulli sampling visits all pages
- Enables page-mode visibility checking only for sampling percentages >= 25% (optimization based on experimentation)
- The cutoff calculation provides strictly correct behavior at probability limits (0 or 1)
- This is a static function, only callable within the bernoulli.c module