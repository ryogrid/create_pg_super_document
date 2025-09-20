# hyperLogLogState

## Location
[src/include/lib/hyperloglog.h:53-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/hyperloglog.h#L53-L60)

## Overview
The  struct represents the state of a HyperLogLog cardinality estimator, which provides an approximate technique for computing the number of distinct entries in a set using a fixed amount of memory.

## Definition

```c
typedef struct hyperLogLogState
{
	uint8		registerWidth;
	Size		nRegisters;
	double		alphaMM;
	uint8	   *hashesArr;
	Size		arrSize;
} hyperLogLogState;
```
## Detailed Description
The  structure maintains the complete state needed for HyperLogLog cardinality estimation, implementing the algorithm described in the 2007 paper "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm". This structure is used throughout PostgreSQL for approximate distinct counting operations, particularly in aggregate operations where exact counting would be memory-prohibitive.

The HyperLogLog algorithm works by maintaining a set of registers that track the maximum number of leading zeros seen in hash values for each register. The cardinality estimate is derived from these register values using statistical methods. PostgreSQL's implementation uses a sparse representation for efficiency and provides fixed space overhead regardless of the input set size.

The structure is designed to work with hash values that are processed through the  function, and cardinality estimates are computed via . The implementation is based on Hideaki Ohno's C++ version and is optimized for PostgreSQL's memory management and data processing patterns.

## Parameters / Member Variables
- : The register width in bits (referred to as "k" in HyperLogLog literature), determining the number of registers as 2^registerWidth. Must be between 4 and 16 inclusive.
- : The total number of registers in the estimator, calculated as 2^registerWidth. This determines the precision and memory usage of the estimator.
- : A bias correction factor calculated as alpha × m², where m is the number of registers. This corrects systematic multiplicative bias in the raw estimator.
- : Pointer to the array storing hash register values. Each register stores the maximum number of leading zeros observed for hash values mapped to that register.
- : The allocated size of the hashesArr in bytes, calculated as sizeof(uint8) × nRegisters + 1.

## Dependencies
- Functions called/Symbols referenced:
  - Size (PostgreSQL type)
  - uint8 (PostgreSQL type)
- Called from (representative examples):
  - initHyperLogLog
  - initHyperLogLogError
  - addHyperLogLog
  - estimateHyperLogLog
  - freeHyperLogLog
  - [hashagg_spill_init](hashagg_spill_init.md)
  - [HashAggSpill](../H/HashAggSpill.md)

## Notes and Other Information
- The implementation uses a sparse representation for efficiency, particularly beneficial when dealing with small to medium-sized sets
- Register width constraints (4-16 bits) balance memory usage with estimation accuracy
- The alphaMM value uses predefined constants for common register counts (16, 32, 64) and calculates dynamically for others
- Memory allocation for hashesArr is done via PostgreSQL's palloc0() to ensure zero-initialization
- The structure is commonly used in PostgreSQL's aggregate operations, particularly for hash-based aggregation with spilling
- Based on MIT-licensed code from Hideaki Ohno, adapted for PostgreSQL's memory management and coding conventions