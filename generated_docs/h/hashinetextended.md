# hashinetextended

## Location
src/backend/utils/adt/network.c: 890 - 902

## Overview
Provides extended hash function support for inet/cidr data types with a seed value, enabling more sophisticated hash indexing operations.

## Definition
```c
Datum hashinetextended(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is an extended version of the hashinet function that supports seeded hashing for inet and cidr data types. It takes both the network address and a 64-bit seed value as parameters, allowing for hash computations that can be varied based on the seed. This is particularly useful for hash joins and other operations where consistent but different hash values are needed across different contexts.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access two arguments:
  - First argument: inet/cidr value retrieved using `PG_GETARG_INET_PP(0)`
  - Second argument: 64-bit seed value retrieved using `PG_GETARG_INT64(1)`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP - retrieves inet argument from function call
  - ip_addrsize - determines the size of the IP address portion
  - hash_any_extended - PostgreSQL's seeded hash function
  - VARDATA_ANY - macro to get variable-length data portion
  - PG_GETARG_INT64 - retrieves the 64-bit seed parameter
- Called from (representative examples):
  - No direct references found in the codebase (likely called via function pointer in extended hash operations)

## Notes and Other Information
- This is the extended variant of hashinet that accepts a seed parameter
- The seed value allows for generating different hash values for the same inet/cidr input
- Like hashinet, it assumes no padding bytes in the inet data structure
- Used primarily in advanced hash operations like hash joins where multiple hash functions with different seeds are needed
- The function computes the hash over the address size plus 2 additional bytes for metadata