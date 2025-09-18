# unionkey

## Location
[src/backend/utils/adt/tsgistidx.c:374-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L374-L401)

## Overview
The unionkey function performs bitwise union operations on TSVector signature keys, used internally by PostgreSQL's GiST index implementation for text search vectors.

## Definition


## Detailed Description
This function merges two TSVector signatures by performing a bitwise OR operation. It handles two different signature formats: signature keys (bit vectors) and array-based representations. When the input signature is marked as ALLTRUE, it returns 1 to indicate that the result should be treated as an all-true signature. For regular signature keys, it performs bitwise OR operations byte by byte. For array-based signatures, it hashes each element and sets the corresponding bits in the base signature.

## Parameters / Member Variables
- : Base signature bit vector that will be modified with the union result
- : TSVector signature to be merged into the base signature  
- : Length of the signature in bytes

## Dependencies
- Functions called/Symbols referenced:
  - ISSIGNKEY (macro to check if signature is a key type)
  - GETSIGN (macro to get signature bit vector)
  - ISALLTRUE (macro to check if signature represents all-true state)
  - GETSIGLEN (macro to get signature length)
  - LOOPBYTE (macro for byte-wise iteration)
  - GETARR (macro to get array representation)
  - ARRNELEM (macro to get number of array elements)
  - [HASH](../H/HASH.md) (macro to hash values into signature)
- Called from:
  - [gtsvector_union](../g/gtsvector_union.md) (GiST union function for TSVector)

## Notes and Other Information
This is a static function used specifically within the TSVector GiST index implementation. The function returns 1 when the result should be treated as an all-true signature (covering all possible values), and 0 for normal signature processing. The function modifies the base signature in-place, making it an efficient union operation for index maintenance.