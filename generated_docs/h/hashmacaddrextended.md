# hashmacaddrextended

## Location
[src/backend/utils/adt/mac.c:275-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L275-L286)

## Overview
The  function computes an extended hash value for a MAC address using a seed value, providing enhanced hash distribution for advanced hash-based operations.

## Definition


## Detailed Description
This function implements an extended hash function for the  data type in PostgreSQL. It takes a MAC address and a seed value as inputs and computes a hash value using PostgreSQL's  function. The extended hash function provides better hash distribution and is used in advanced hash-based operations such as parallel hash joins and hash partitioning.

The seed parameter allows for creating different hash values for the same MAC address, which is useful in scenarios where multiple independent hash functions are needed, such as in parallel processing or when avoiding hash collisions in specific contexts.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - The MAC address to hash
  - Second argument:  - Seed value for extended hashing

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract MAC address argument
  - : Macro to extract the 64-bit seed value
  - : Extended hash function that uses a seed for enhanced distribution
  - : Size of the MAC address structure
- Called from (representative examples):
  - No direct callers found (used by PostgreSQL's advanced hash infrastructure)

## Notes and Other Information
- This function extends the basic  functionality by incorporating a seed value
- Used in advanced hash-based operations like parallel hash joins and hash partitioning
- The seed parameter enables creation of different hash families from the same data
- Essential for avoiding systematic hash collisions in parallel processing scenarios
- Located in 
- Provides better hash distribution compared to the basic hash function
- Part of PostgreSQL's extended hash function family introduced for improved parallel processing capabilities