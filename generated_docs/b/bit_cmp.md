# bit_cmp

## Location
[src/backend/utils/adt/varbit.c:818-840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L818-L840)

## Overview
Internal comparison function for bit string types that performs lexicographic comparison between two VarBit structures, considering both bit content and length.

## Definition

```c
static int32
bit_cmp(VarBit *arg1, VarBit *arg2)
```
## Detailed Description
The  function is a core comparison routine used by all bit string comparison operators in PostgreSQL. It performs a comprehensive comparison between two bit strings, taking into account both the bit content and the actual length of the strings. The function implements lexicographic ordering where strings are compared byte-by-byte first, and if the common prefix is identical, the shorter string is considered smaller.

The comparison is designed to handle both fixed-length BIT and variable-length VARBIT types uniformly, as they share the same internal representation. The function is specifically designed to be memory-efficient for btree index operations and avoids memory leaks by working directly with the input structures without creating temporary copies.

A key design consideration is that trailing zeros are significant - a bit string "101" is different from "1010" even though their numerical values might be considered equal in some contexts. This ensures that the database maintains exact bit string semantics.

## Parameters / Member Variables
-  (VarBit*): First bit string to compare
-  (VarBit*): Second bit string to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the byte length of the bit string data
  - : Macro to get pointer to the actual bit data
  - : Macro to get the bit length of the string
  - : Standard C library function for memory comparison
  - : PostgreSQL macro for minimum value

- Called from (representative examples):
  - : Equality comparison operator
  - : Inequality comparison operator
  - : Less than comparison operator
  - : Less than or equal comparison operator
  - : Greater than comparison operator
  - : Greater than or equal comparison operator
  - : Public comparison function wrapper

## Notes and Other Information
- Returns negative value if arg1 < arg2, zero if equal, positive if arg1 > arg2
- Uses byte-wise comparison first for efficiency, then compares bit lengths if content is identical
- Designed to be memory-leak free for btree index usage
- Handles both BIT and VARBIT types with the same logic due to identical internal representation
- Trailing bits and padding are significant in comparisons - "1" ≠ "10" even if numerically equivalent
- The function is static and not directly exposed to SQL, but used by all bit string comparison operators
- Located in src/backend/utils/adt/varbit.c:818-840

## Simplified Source

```c
static int32 bit_cmp(VarBit *arg1, VarBit *arg2) {
    int bytelen1 = VARBITBYTES(arg1);
    int bytelen2 = VARBITBYTES(arg2);

    // Compare common bytes first
    int32 cmp = memcmp(VARBITS(arg1), VARBITS(arg2), Min(bytelen1, bytelen2));

    // If bytes are equal, compare by length
    if (cmp == 0) {
        int bitlen1 = VARBITLEN(arg1);
        int bitlen2 = VARBITLEN(arg2);
        if (bitlen1 != bitlen2)
            cmp = (bitlen1 < bitlen2) ? -1 : 1;
    }

    return cmp;  // <0, 0, >0 for less, equal, greater
}
```