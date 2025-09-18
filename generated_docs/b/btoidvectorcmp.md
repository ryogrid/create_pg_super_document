# btoidvectorcmp

## Location
[src/backend/access/nbtree/nbtcompare.c:296-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtcompare.c#L296-L319)

## Overview
A B-tree comparison function for PostgreSQL's oidvector data type that compares two oidvector values by first comparing their lengths and then performing element-wise comparison.

## Definition
```c
Datum btoidvectorcmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The btoidvectorcmp function is a B-tree comparison function for the oidvector data type in PostgreSQL. An oidvector is an array-like structure that stores multiple OID values, commonly used for representing lists of object identifiers such as function argument types. The function implements a lexicographic comparison strategy: it first compares the lengths (dimensions) of the two vectors, and if they are equal, it performs element-by-element comparison until a difference is found. This ordering ensures that shorter vectors are considered "less than" longer vectors, and vectors of equal length are compared by their content in array order.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL's standard function argument interface containing:
  - First argument (index 0): oidvector pointer 'a' - the first vector to compare
  - Second argument (index 1): oidvector pointer 'b' - the second vector to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER: Extracts pointer arguments from function call
  - PG_RETURN_INT32: Returns 32-bit integer result as Datum
  - A_GREATER_THAN_B: Constant indicating first argument is greater
  - A_LESS_THAN_B: Constant indicating first argument is less than second
  - oidvector: The PostgreSQL data type for arrays of OIDs

- Called from (representative examples):
  - [oidvectoreq](../o/oidvectoreq.md): OID vector equality comparison function
  - [oidvectorne](../o/oidvectorne.md): OID vector not-equal comparison function  
  - [oidvectorlt](../o/oidvectorlt.md): OID vector less-than comparison function
  - [oidvectorle](../o/oidvectorle.md): OID vector less-than-or-equal comparison function
  - [oidvectorge](../o/oidvectorge.md): OID vector greater-than-or-equal comparison function
  - [oidvectorgt](../o/oidvectorgt.md): OID vector greater-than comparison function

## Notes and Other Information
- Uses a two-phase comparison strategy: first by length, then by content
- Shorter vectors are always considered less than longer vectors regardless of content
- Element-wise comparison stops at the first difference found
- Returns the same comparison semantics as other PostgreSQL B-tree comparison functions
- The dim1 field represents the number of elements in the oidvector
- The values array contains the actual OID values stored in the vector
- Used by various OID vector comparison operators and B-tree indexing operations