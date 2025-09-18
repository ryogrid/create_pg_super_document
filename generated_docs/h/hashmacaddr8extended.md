# hashmacaddr8extended

## Location
[src/backend/utils/adt/mac8.c:403-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L403-L414)

## Overview
The hashmacaddr8extended function is an extended hash support function for creating hash indexes on PostgreSQL's 8-byte MAC address (macaddr8) data type, supporting seed-based hashing.

## Definition


## Detailed Description
This function computes a seeded hash value for a macaddr8 (8-byte MAC address) value, enabling the use of hash indexes and hash joins for this data type with additional randomization through a seed value. The function uses PostgreSQL's hash_any_extended function to compute a hash over the entire 8-byte MAC address structure with a provided seed. The extended version provides better hash distribution and security by allowing different hash seeds, which is particularly useful for preventing hash collision attacks and improving performance in certain scenarios.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: The macaddr8 value to hash (key)
  - Argument 1: A 64-bit seed value for the hash function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro to extract macaddr8 argument)
  - PG_GETARG_INT64 (macro to extract 64-bit seed argument)
  - [hash_any_extended](hash_any_extended.md) (generic seeded hash function for arbitrary byte sequences)
  - sizeof (operator to get size of macaddr8 type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically registered as the extended hash support function for macaddr8 data type in PostgreSQL's system catalogs
- The hash is computed over the entire 8-byte MAC address structure with the provided seed for consistent but randomized results
- Returns a 64-bit hash value as a Datum
- The extended version provides better security and performance characteristics compared to the basic hash function
- Essential for modern hash-based query execution plans involving macaddr8 columns
- Part of PostgreSQL's MAC address data type support introduced for 8-byte MAC addresses