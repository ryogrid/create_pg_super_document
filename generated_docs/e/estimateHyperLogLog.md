# estimateHyperLogLog

## Location
src/backend/lib/hyperloglog.c: 186 - 241

## Overview
Computes the final cardinality estimate from a HyperLogLog state, applying range corrections for improved accuracy in small and large cardinality ranges.

## Definition
```c
double estimateHyperLogLog(hyperLogLogState *cState)
```

## Detailed Description
This function implements the core HyperLogLog cardinality estimation algorithm with range corrections as described in the original HyperLogLog paper. It processes the register array to compute a raw estimate using the harmonic mean of the register values, then applies corrections for small and large cardinality ranges where the basic algorithm shows bias.

The function first calculates the raw estimate using the formula E = αm² / Σ(2^(-M[j])) where αm² is the pre-calculated bias correction factor and M[j] are the register values. For improved accuracy, it applies small range correction when the estimate is small relative to the number of registers (using the number of zero registers), and large range correction when the estimate approaches the theoretical maximum for 32-bit hashes.

## Parameters / Member Variables
- `cState`: Pointer to the hyperLogLogState structure containing the register array and pre-calculated constants

## Dependencies
- Functions called/Symbols referenced:
  - pow (mathematical power function)
  - log (natural logarithm function)
  - POW_2_32 (constant: 2^32)
  - NEG_POW_2_32 (constant: -2^32)
  - hyperLogLogState (structure type)
- Called from (representative examples):
  - hashagg_spill_finish
  - macaddr_abbrev_abort
  - network_abbrev_abort
  - numeric_abbrev_abort
  - uuid_abbrev_abort
  - varstr_abbrev_abort

## Notes and Other Information
- Implements the complete HyperLogLog algorithm including both small and large range corrections
- Small range correction (≤ 2.5m) uses the coupon collector problem formula when zero registers exist
- Large range correction (> 2^32/30) compensates for saturation effects with 32-bit hashes
- The raw estimate uses harmonic mean computation over all register values
- Widely used in PostgreSQL abbreviation abort decisions and hash aggregation spilling
- Returns a floating-point cardinality estimate that approximates the number of distinct values processed
- The range corrections significantly improve accuracy compared to the raw HyperLogLog formula
- Performance implications: involves floating-point operations over all registers