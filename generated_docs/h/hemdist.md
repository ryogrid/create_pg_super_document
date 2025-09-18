# hemdist

## Location
[src/backend/utils/adt/tsquery_gist.c:131-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_gist.c#L131-L138)

## Overview
Calculates the Hamming distance between two text search vector signatures, which represents the dissimilarity between document signatures in the GiST indexing system for full-text search.

## Definition


## Detailed Description
The hemdist function computes the Hamming distance between two SignTSVector signatures, which is a measure of how different two bit signatures are. This function is essential for GiST (Generalized Search Tree) index operations on text search vectors, particularly for penalty calculation and node splitting decisions.

The function handles three cases:
1. If both signatures are ALLTRUE (representing very common terms), the distance is 0
2. If one signature is ALLTRUE and the other is not, it calculates the distance based on the number of unset bits in the non-ALLTRUE signature
3. For normal signatures of equal length, it delegates to hemdistsign for actual bit-by-bit comparison

## Parameters / Member Variables
- `a`: Pointer to the first SignTSVector signature for comparison
- `b`: Pointer to the second SignTSVector signature for comparison

## Dependencies
- Functions called/Symbols referenced:
  - GETSIGLEN (macro to get signature length)
  - ISALLTRUE (macro to check if signature represents all-true state)
  - SIGLENBIT (macro to calculate total bits in signature)
  - [sizebitvec](../s/sizebitvec.md) (function to count set bits in signature)
  - GETSIGN (macro to get signature data)
  - [hemdistsign](hemdistsign.md) (function to calculate Hamming distance between signatures)
- Called from:
  - [gtsvector_penalty](../g/gtsvector_penalty.md)
  - [gtsquery_penalty](../g/gtsquery_penalty.md)
  - [gtsquery_picksplit](../g/gtsquery_picksplit.md)

## Notes and Other Information
- This function is static and only accessible within the tsgistidx.c compilation unit
- The ALLTRUE optimization handles cases where signatures represent very common terms that would match most documents
- Essential for GiST index performance in text search operations
- Located in src/backend/utils/adt/tsgistidx.c:512-532