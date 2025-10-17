# makeTSQuerySign

## Location
[src/backend/utils/adt/tsquery_op.c:250-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L250-L266)

## Overview
Generates a bit signature for a TSQuery by extracting and hashing all value operands, used for fast filtering in GiST indexing.

## Definition

```c
TSQuerySign
makeTSQuerySign(TSQuery a)
```
## Detailed Description
The  function creates a compact bit signature (TSQuerySign) from a TSQuery object by examining all value operands (QI_VAL type) within the query. For each value operand found, it extracts the precomputed CRC hash () from the operand and sets the corresponding bit in the signature based on the hash value modulo the signature length.

This signature is primarily used by PostgreSQL's GiST (Generalized Search Tree) indexing system for tsquery types. The signature provides a quick way to eliminate obviously non-matching queries during index searches, improving query performance by avoiding expensive full comparisons when possible.

The function iterates through all QueryItem elements in the TSQuery, but only processes value operands (QI_VAL), ignoring operators (QI_OPR). The resulting signature is a bitwise OR of all individual hash positions, creating a lossy but efficient representation of the query's content.

## Parameters / Member Variables
- `a`: The TSQuery object to create a signature for
## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY (macro to get QueryItem array from TSQuery)
  - TSQS_SIGLEN (constant defining signature length in bits)
- Data structures used:
  - TSQuery
  - TSQuerySign
  - QueryItem
  - QI_VAL (query item type for values)

- Called from:
  - [gtsquery_compress](../g/gtsquery_compress.md) (in tsquery_gist.c)
  - [gtsquery_consistent](../g/gtsquery_consistent.md) (in tsquery_gist.c)
  - PG_GETARG_TSQUERYSIGN macro

## Notes and Other Information
- Returns a TSQuerySign (typically an integer type used as a bit vector)
- Only processes value operands (QI_VAL), ignoring query operators
- Uses precomputed CRC values (valcrc) from operands for efficiency
- Signature length is TSQS_SIGLEN bits (sizeof(TSQuerySign) * 8)
- Multiple different operands may map to the same bit (hash collision is acceptable)
- Essential component of PostgreSQL's GiST indexing for full-text search
- Provides lossy compression suitable for index filtering, not exact matching

## Simplified Source

```c
TSQuerySign
makeTSQuerySign(TSQuery a)
{
    TSQuerySign signature = 0;
    QueryItem *items = GETQUERY(a);

    // Process each query item
    for (int i = 0; i < a->size; i++)
    {
        // Only hash value operands, not operators
        if (items[i].type == QI_VAL)
        {
            // Set bit based on CRC hash modulo signature length
            int bit_pos = items[i].qoperand.valcrc % TSQS_SIGLEN;
            signature |= ((TSQuerySign) 1) << bit_pos;
        }
    }

    return signature;
}
```