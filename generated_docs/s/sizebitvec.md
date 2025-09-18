# sizebitvec

## Location
[src/backend/utils/adt/tsgistidx.c:490-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L490-L495)

## Overview
The sizebitvec function counts the number of set bits (1s) in a bit vector signature, used for calculating Hamming distances and other bit vector operations in PostgreSQL's TSVector GiST index implementation.

## Definition


## Detailed Description
This function provides a simple wrapper around PostgreSQL's pg_popcount() function to count the number of set bits in a bit vector signature. It's a utility function used throughout the TSVector GiST index implementation for calculating bit densities, Hamming distances, and making index optimization decisions. The function is essential for measuring the 'size' or cardinality of signature bit vectors.

## Parameters / Member Variables
- : Bit vector pointer (BITVECP) to the signature whose bits should be counted
- : Length of the signature in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pg_popcount](../p/pg_popcount.md) (PostgreSQL function to count set bits in memory)
  - BITVECP (typedef for bit vector pointer)
- Called from (representative examples):
  - [gtsvectorout](../g/gtsvectorout.md) (for signature output formatting)
  - [hemdist](../h/hemdist.md) (for Hamming distance calculations)
  - [gtsvector_penalty](../g/gtsvector_penalty.md) (for penalty calculations in index operations)
  - [hemdistcache](../h/hemdistcache.md) (for cached distance calculations)
  - [gtsvector_picksplit](../g/gtsvector_picksplit.md) (for page splitting decisions)

## Notes and Other Information
This is a static helper function that abstracts the bit counting operation, making the code more readable and maintainable. The function leverages PostgreSQL's optimized pg_popcount() implementation which uses hardware instructions when available for efficient bit counting. It's widely used throughout the TSVector GiST implementation wherever bit density measurements are needed.