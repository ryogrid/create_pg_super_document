# HASH

## Location
[src/backend/regex/regexec.c:49-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L49-L49)

## Overview
HASH is a macro definition in PostgreSQL's regular expression engine that provides an optimized method for computing hash values of bitvectors used in DFA state representation.

## Definition


## Detailed Description
The HASH macro is an optimization for computing hash values of bitvectors in PostgreSQL's regular expression DFA implementation. It provides a fast path for single-word bitvectors by directly using the word value as the hash, while falling back to a full hash function for multi-word bitvectors. This optimization is crucial for performance in the DFA state caching and lookup mechanisms, where hash values are frequently computed to quickly identify and retrieve cached states.

## Parameters / Member Variables
- : Pointer to the bitvector (array of unsigned integers) to be hashed
- : Number of words in the bitvector

## Dependencies
- Functions called/Symbols referenced:
  - [hash](../h/hash.md) (hash function for multi-word bitvectors when nw > 1)
- Called from (representative examples):
  - [initialize](../i/initialize.md) (DFA state initialization)
  - [miss](../m/miss.md) (DFA cache miss handling)
  - [makesign](../m/makesign.md) (text search GiST indexing)
  - [unionkey](../u/unionkey.md) (text search GiST key union)

## Notes and Other Information
This macro demonstrates a common optimization pattern in PostgreSQL where special cases (single-word bitvectors) are handled with minimal overhead, while more complex cases fall back to general-purpose functions. The optimization is particularly important in regex matching where state lookup performance directly affects overall pattern matching speed. The macro is also used in other parts of PostgreSQL beyond the regex engine, such as in text search GiST indexing operations.