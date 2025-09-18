# RWConflictPoolHeader

## Location
[src/include/storage/predicate_internals.h:212-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L212-L213)

## Overview
A pointer type definition that provides convenient access to RWConflictPoolHeaderData structures used in PostgreSQL's serializable isolation conflict management system.

## Definition


## Detailed Description
RWConflictPoolHeader is a typedef that creates a pointer type for RWConflictPoolHeaderData structures. This type alias provides a cleaner, more convenient way to work with pointers to conflict pool headers throughout the PostgreSQL serializable isolation implementation. By using this typedef, the code becomes more readable and maintainable, as developers can use RWConflictPoolHeader instead of the more verbose 'struct RWConflictPoolHeaderData *' syntax. This pattern is commonly used in PostgreSQL's codebase to create convenient handle types for complex data structures.

## Parameters / Member Variables
- This is a pointer typedef, so it points to RWConflictPoolHeaderData structures which contain:
  - : Doubly-linked list of available conflict objects
  - : Base RWConflict object in the pool

## Dependencies
- Functions called/Symbols referenced:
  - [RWConflictPoolHeaderData](RWConflictPoolHeaderData.md) (the underlying structure type)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md) (uses this type in serialization control structure)

## Notes and Other Information
- This is a standard PostgreSQL pattern of creating pointer typedefs for cleaner code
- Part of the internal predicate locking system for serializable snapshot isolation
- Simplifies function signatures and variable declarations when working with conflict pool headers
- The underlying structure manages memory pools for efficient allocation of conflict tracking objects