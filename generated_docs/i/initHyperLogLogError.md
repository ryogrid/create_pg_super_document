# initHyperLogLogError

## Location
src/backend/lib/hyperloglog.c: 128 - 150

## Overview
Initializes a HyperLogLog state structure by specifying a target error rate instead of bit width, automatically calculating the optimal bit width to achieve the desired accuracy.

## Definition
```c
void initHyperLogLogError(hyperLogLogState *cState, double error)
```

## Detailed Description
This function provides an alternative initialization method for HyperLogLog state structures, allowing users to specify a desired error rate rather than the technical bit width parameter. It uses the theoretical relationship from the HyperLogLog paper (e = 1.04 / sqrt(m), where m is the number of registers) to determine the minimum bit width that will achieve an error rate below the specified threshold.

The function iterates through possible bit widths starting from 4, calculating the theoretical error rate for each, until it finds the first bit width that provides an error rate below the target. It then delegates to initHyperLogLog with the calculated bit width.

## Parameters / Member Variables
- `cState`: Pointer to the hyperLogLogState structure to be initialized
- `error`: Target error rate (as a decimal, e.g., 0.01 for 1% error rate)

## Dependencies
- Functions called/Symbols referenced:
  - initHyperLogLog (for actual initialization with calculated bit width)
  - sqrt (mathematical function for square root calculation)
  - hyperLogLogState (structure type)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- The error rate range is constrained by the bit width limits: approximately 25% error (bwidth=4) to 0.4% error (bwidth=16)
- Uses the standard HyperLogLog error formula: e = 1.04 / sqrt(m) where m = 2^bwidth
- Provides a more user-friendly interface compared to specifying bit width directly
- The function finds the lowest bit width that satisfies the error requirement, optimizing for memory usage
- Currently appears to be unused in the PostgreSQL codebase, possibly provided for future extensibility or external API usage