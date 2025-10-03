# addHyperLogLog

## Location
[src/backend/lib/hyperloglog.c:167-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/hyperloglog.c#L167-L185)

## Overview
Processes a hash value and updates the HyperLogLog state to incorporate a new element for cardinality estimation.

## Definition
```c
void addHyperLogLog(hyperLogLogState *cState, uint32 hash)
```

## Detailed Description
This function is the core operation of the HyperLogLog algorithm, responsible for processing individual hash values and updating the internal register array. It takes a 32-bit hash value, splits it into two parts: the first k bits (where k is the registerWidth) serve as an index to select which register to update, and the remaining (32-k) bits are used to compute a "rank" value using the rho function.

The algorithm maintains the maximum rank seen for each register index, which forms the basis for cardinality estimation. The rank represents the position of the first set bit in the hash suffix, which follows a geometric distribution that enables probabilistic cardinality estimation through stochastic averaging.

It is critical that the input hash values have uniform bit distribution, as the algorithms accuracy depends on the stochastic properties of well-distributed hash functions.

## Parameters / Member Variables
- `cState`: Pointer to the hyperLogLogState structure being updated
- `hash`: A 32-bit hash value that must be uniformly distributed (typically from hash_any())

## Dependencies
- Functions called/Symbols referenced:
  - [rho](../r/rho.md) (calculates position of first set bit)
  - Max (macro for maximum value selection)
  - BITS_PER_BYTE (constant for bit manipulation)
  - [hyperLogLogState](../h/hyperLogLogState.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - [macaddr_abbrev_convert](../m/macaddr_abbrev_convert.md)
  - [network_abbrev_convert](../n/network_abbrev_convert.md)
  - [numeric_abbrev_convert_var](../n/numeric_abbrev_convert_var.md)
  - [uuid_abbrev_convert](../u/uuid_abbrev_convert.md)
  - [varstr_abbrev_convert](../v/varstr_abbrev_convert.md)

## Notes and Other Information
- The hash input must be from a quality hash function like hash_any() for algorithm correctness
- Uses bit shifting to extract register index from high-order bits of the hash
- Updates registers using maximum operation - each register stores the highest rank seen for its index
- Widely used in PostgreSQL abbreviation conversion functions for different data types
- The register update follows the classic HyperLogLog algorithm where each bucket maintains the maximum rank
- Performance-critical function as its called once per distinct value being tracked

## Simplified Source

```c
void
addHyperLogLog(hyperLogLogState *cState, uint32 hash)
{
    // Extract register index from high-order bits
    uint32 index = hash >> (BITS_PER_BYTE * sizeof(uint32) - cState->registerWidth);

    // Calculate rank (position of first set bit) in remaining bits
    uint8 count = rho(hash << cState->registerWidth,
                      BITS_PER_BYTE * sizeof(uint32) - cState->registerWidth);

    // Keep maximum rank seen for this register
    cState->hashesArr[index] = Max(count, cState->hashesArr[index]);
}
```