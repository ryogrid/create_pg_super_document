# gtsvector_penalty

## Location
[src/backend/utils/adt/tsgistidx.c:533-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L533-L572)

## Overview
Calculates the penalty cost for inserting a new tsvector entry into a GiST index node, used to determine the optimal insertion point that minimizes index tree expansion.

## Definition
```c
Datum gtsvector_penalty(PG_FUNCTION_ARGS)
```

## Detailed Description
The `gtsvector_penalty` function is a PostgreSQL GiST index support function that calculates the penalty (cost) of inserting a new text search vector entry into an existing index node. This penalty calculation is crucial for maintaining an efficient GiST index structure by choosing insertion points that minimize the expansion of index node boundaries.

The function handles two types of new entries:
1. **Array key entries (ISARRKEY)**: Converts the array to a signature and calculates penalty based on bit differences
2. **Signature entries**: Directly calculates Hamming distance between signatures

For ALLTRUE original signatures (highly compressed), it uses a specialized formula considering the density of bits in the new signature. For regular signatures, it uses the Hamming distance calculation.

## Parameters / Member Variables
- `origentry`: Original GiST entry in the index node (always a signature key)
- `newentry`: New entry to be inserted
- `penalty`: Output parameter to store the calculated penalty value

## Dependencies
- Functions called/Symbols referenced:
  - GET_SIGLEN (macro to get signature length)
  - GETSIGN (macro to get signature bit vector)
  - ISARRKEY (macro to check if entry is array-based)
  - [makesign](../m/makesign.md) (function to create signature from array)
  - ISALLTRUE (macro to check if signature is in ALLTRUE state)
  - SIGLENBIT (macro to get bit length of signature)
  - [sizebitvec](../s/sizebitvec.md) (function to count set bits)
  - [hemdistsign](../h/hemdistsign.md) (function for Hamming distance between bit vectors)
  - [hemdist](../h/hemdist.md) (function for Hamming distance between signatures)
  - [palloc](../p/palloc.md)/pfree (memory allocation functions)
- Called from:
  - Not directly called (used as GiST support function via function pointer)

## Notes and Other Information
- This is a PostgreSQL GiST index support function registered in the operator class
- The penalty value influences GiST index tree structure and query performance
- Lower penalty values indicate better insertion choices
- Used during INSERT operations and index building
- The function uses different penalty calculation strategies based on signature types
- Memory management includes temporary allocation and cleanup for signature conversion
- Located in src/backend/utils/adt/tsgistidx.c:533-572

## Simplified Source

```c
Datum gtsvector_penalty(PG_FUNCTION_ARGS) {
    GISTENTRY *origentry = (GISTENTRY *) PG_GETARG_POINTER(0);
    GISTENTRY *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
    float *penalty = (float *) PG_GETARG_POINTER(2);

    int siglen = GET_SIGLEN();
    SignTSVector *origval = (SignTSVector *) DatumGetPointer(origentry->key);
    SignTSVector *newval = (SignTSVector *) DatumGetPointer(newentry->key);
    BITVECP orig = GETSIGN(origval);

    *penalty = 0.0;

    if (ISARRKEY(newval)) {
        // Convert array to signature for comparison
        BITVECP sign = palloc(siglen);
        makesign(sign, newval, siglen);

        if (ISALLTRUE(origval)) {
            // Special penalty calculation for ALLTRUE signatures
            int siglenbit = SIGLENBIT(siglen);
            *penalty = (float)(siglenbit - sizebitvec(sign, siglen)) /
                      (float)(siglenbit + 1);
        } else {
            // Standard Hamming distance for regular signatures
            *penalty = hemdistsign(sign, orig, siglen);
        }

        pfree(sign);  // Clean up temporary signature
    } else {
        // Both are signatures - direct comparison
        *penalty = hemdist(origval, newval);
    }

    PG_RETURN_POINTER(penalty);
}
```