# hashtidextended

## Location
[src/backend/utils/adt/tid.c:272-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L272-L295)

## Overview
A PostgreSQL function that computes an extended hash value for a tuple identifier (TID) using a seed value, providing enhanced hashing capabilities for advanced hash-based operations.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that generates a seeded hash value from an ItemPointer (TID). This is an extended version of the  function that accepts an additional 64-bit seed parameter. It uses the  function to compute the hash, which allows for better hash distribution and is particularly useful in advanced hashing scenarios such as hash joins with multiple hash stages or hash partitioning. Like its simpler counterpart, it carefully calculates the size by adding the sizes of component fields ( + ) rather than using  to avoid potential padding issues.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - : ItemPointer - the TID value to hash
  - : uint64 - a 64-bit seed value for the hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (macro for extracting ItemPointer argument)
  - PG_GETARG_INT64 (macro for extracting 64-bit integer seed)
  - [hash_any_extended](hash_any_extended.md) (extended hash function with seed support)
  - [BlockIdData](../B/BlockIdData.md) (type representing block identifier component)
  - OffsetNumber (type representing offset component)
- Called from:
  - No direct references found (likely used through extended hash operator classes for TID type)

## Notes and Other Information
- Extended version of  with seed support for advanced hashing scenarios
- Essential for sophisticated hash-based operations requiring seed-based hashing
- Maintains the same careful approach to size calculation as  for portability
- The seed parameter enables better hash distribution and collision avoidance
- Used in complex hash operations like multi-level hash joins
- Part of PostgreSQL's extended hash operator family for the TID data type
- Located in src/backend/utils/adt/tid.c:272-295