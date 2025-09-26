# initHyperLogLog

## Location
src/backend/lib/hyperloglog.c: 66 - 127

## Overview
Initializes a HyperLogLog state structure for probabilistic cardinality estimation with a specified bit width parameter.

## Definition
```c
void initHyperLogLog(hyperLogLogState *cState, uint8 bwidth)
```

## Detailed Description
This function initializes a HyperLogLog state structure, which is used for probabilistic cardinality estimation algorithms. The HyperLogLog algorithm provides approximate distinct count estimates with configurable precision controlled by the bit width parameter. The function sets up the register array, calculates the bias correction factor (alpha), and prepares all necessary state for subsequent hash value processing.

The bit width determines the number of registers (2^bwidth) and directly impacts both memory usage and estimation accuracy. Higher bit widths provide more accurate estimates but consume more memory. The function validates the bit width is within the supported range of 4-16 bits inclusive.

## Parameters / Member Variables
- `cState`: Pointer to the hyperLogLogState structure to be initialized
- `bwidth`: Bit width for the HyperLogLog algorithm (must be between 4 and 16 inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - palloc0 (for zero-initialized memory allocation)
  - hyperLogLogState (structure type)
- Called from (representative examples):
  - hashagg_spill_init
  - initHyperLogLogError
  - macaddr_sortsupport
  - network_sortsupport
  - numeric_sortsupport
  - uuid_sortsupport
  - varstr_sortsupport

## Notes and Other Information
- The function calculates alpha correction factors for specific register counts (16, 32, 64) with hardcoded values, and uses a general formula for other counts
- The alpha correction factor addresses systematic multiplicative bias in the raw HyperLogLog estimate
- The hashesArr is initialized to zero (not negative infinity) following the coupon collector problem discussion in the HyperLogLog paper
- Precalculates alphaMM (alpha * m^2) for efficient raw estimate generation
- Widely used in PostgreSQL for sort support operations and hash aggregation spilling