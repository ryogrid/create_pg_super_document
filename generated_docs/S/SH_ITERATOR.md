# SH_ITERATOR

## Location
[src/include/lib/simplehash.h:181-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L181-L186)

## Overview
SH_ITERATOR is a macro that generates the iterator type name for specialized hash table implementations in PostgreSQL's templated simplehash system.

## Definition
```c
#define SH_ITERATOR SH_MAKE_NAME(iterator)
```

## Detailed Description
SH_ITERATOR is a macro in PostgreSQL's simplehash.h templating system that creates the iterator type name for hash table traversal operations. When used with a prefix (defined by SH_PREFIX), it generates an iterator type name following the pattern `<prefix>_iterator`. This iterator provides a mechanism for efficiently traversing all elements in a hash table without requiring knowledge of the internal bucket structure.

The iterator is designed to work with the open-addressing hash table implementation and supports both full table iteration and iteration starting from a specific position. It maintains internal state to track the current position, end boundary, and completion status. The iterator design is optimized for the Robin Hood hashing scheme used by simplehash, ensuring efficient traversal even with the complex bucket displacement patterns that can occur during hash table operations.

The iterator structure provides a safe way to traverse hash table contents without exposing the underlying bucket array structure to client code. This abstraction allows the hash table implementation to change its internal organization without affecting iteration code.

## Parameters / Member Variables
- No direct parameters (macro definition)
- When instantiated, the resulting SH_ITERATOR structure contains:
  - `cur`: Current element position in the hash table
  - `end`: End boundary for iteration
  - `done`: Boolean flag indicating if iteration is exhausted

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME
- Called from (representative examples):
  - [SH_START_ITERATE](SH_START_ITERATE.md)
  - [SH_START_ITERATE_AT](SH_START_ITERATE_AT.md)  
  - [SH_ITERATE](SH_ITERATE.md)

## Notes and Other Information
- Part of the templated hash table generation system in src/include/lib/simplehash.h:110
- Must be used in conjunction with SH_PREFIX to generate meaningful iterator type names
- The iterator provides safe traversal of hash table elements without exposing internal structure
- Supports both full table iteration and iteration starting from specific positions
- Used with SH_START_ITERATE, SH_START_ITERATE_AT, and SH_ITERATE functions for complete iteration workflows
- Essential for implementing hash table traversal patterns in PostgreSQL's high-performance hash table system