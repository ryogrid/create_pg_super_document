# gtsvectorout

## Location
[src/backend/utils/adt/tsgistidx.c:106-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L106-L134)

## Overview
This function converts a gtsvector (internal GiST signature representation) to its string representation for debugging and display purposes.

## Definition
Datum gtsvectorout(PG_FUNCTION_ARGS)

## Detailed Description
The gtsvectorout function serves as the output function for the gtsvector data type in PostgreSQL's GiST indexing system for tsvector. It converts internal signature representations used by GiST indexes into human-readable strings. The function handles two main types of signatures: array keys (showing element count) and signature keys (showing bit statistics). For signature keys, it can display either "all true bits" for fully set signatures or a count of true/false bits. This function is primarily used for debugging and administrative purposes when examining GiST index internals.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro: Standard PostgreSQL function argument structure, expects one argument which is the gtsvector to convert

## Dependencies
- Functions called/Symbols referenced:
  - [SignTSVector](../S/SignTSVector.md) (data type for signature representation)
  - PG_DETOAST_DATUM (macro for detoasting PostgreSQL datums)
  - ISARRKEY (macro to check if key is array type)
  - ARRNELEM (macro to get array element count)
  - ISALLTRUE (macro to check if all signature bits are true)
  - GETSIGLEN (macro to get signature length)
  - GETSIGN (macro to get signature data)
  - [sizebitvec](../s/sizebitvec.md) (function to count true bits in signature)
  - SIGLENBIT (macro to convert signature length to bits)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - sprintf (string formatting)
- Called from (representative examples):
  - No direct references found (typically called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of the GiST infrastructure for full-text search indexing
- Output format varies based on signature type: array keys show element count, signature keys show bit statistics
- Uses static buffer sizing with outbuf_maxlen for memory efficiency
- The function handles memory management with PG_FREE_IF_COPY for proper cleanup
- Located in src/backend/utils/adt/tsgistidx.c alongside other GiST support functions for tsvector
- Primarily used for debugging and diagnostic purposes rather than end-user operations

## Simplified Source

```c
Datum
gtsvectorout(PG_FUNCTION_ARGS)
{
    SignTSVector *key = (SignTSVector *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    char *outbuf = palloc(outbuf_maxlen);

    if (ISARRKEY(key)) {
        // For array keys: show element count
        sprintf(outbuf, ARROUTSTR, (int) ARRNELEM(key));
    }
    else {
        // For signature keys: show bit statistics
        if (ISALLTRUE(key)) {
            sprintf(outbuf, "all true bits");
        }
        else {
            int siglen = GETSIGLEN(key);
            int true_bits = sizebitvec(GETSIGN(key), siglen);
            int false_bits = SIGLENBIT(siglen) - true_bits;
            sprintf(outbuf, SINGOUTSTR, true_bits, false_bits);
        }
    }

    PG_FREE_IF_COPY(key, 0);
    PG_RETURN_POINTER(outbuf);
}
```