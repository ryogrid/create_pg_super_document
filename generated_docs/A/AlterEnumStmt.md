# AlterEnumStmt

## Location
[src/include/nodes/parsenodes.h:3718-3727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3718-L3727)

## Overview
AlterEnumStmt represents an ALTER TYPE statement for modifying enumeration types in PostgreSQL's parse tree structure.

## Definition
```c
typedef struct AlterEnumStmt
{
    NodeTag     type;
    List       *typeName;         /* qualified name (list of String) */
    char       *oldVal;           /* old enum value's name, if renaming */
    char       *newVal;           /* new enum value's name */
    char       *newValNeighbor;   /* neighboring enum value, if specified */
    bool        newValIsAfter;    /* place new enum value after neighbor? */
    bool        skipIfNewValExists; /* no error if new already exists? */
} AlterEnumStmt;
```

## Detailed Description
AlterEnumStmt is a parse tree node that represents ALTER TYPE statements used to modify existing enumeration types in PostgreSQL. It supports adding new values to an enum, renaming existing values, and controlling the position of new values within the ordered list. The statement provides flexibility for maintaining enum types over time as application requirements evolve, while preserving the ordering semantics that are crucial for enum comparisons.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterEnumStmt node
- `typeName`: List of String nodes representing the qualified name of the enum type to be altered
- `oldVal`: String containing the name of an existing enum value to be renamed (NULL if adding a new value)
- `newVal`: String containing the name of the new enum value or the new name for a renamed value
- `newValNeighbor`: String specifying an existing enum value used as a position reference for the new value (NULL if position not specified)
- `newValIsAfter`: Boolean indicating whether the new value should be placed after (true) or before (false) the neighbor value
- `skipIfNewValExists`: Boolean flag for IF NOT EXISTS behavior - suppresses error if the new value already exists

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [List](../L/List.md) (containing String nodes)
- Called from (representative examples):
  - [AlterEnum](AlterEnum.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Supports both ADD VALUE and RENAME VALUE operations on enum types
- Position control allows precise ordering of new enum values, which is critical since enum comparison depends on ordinal position
- The IF NOT EXISTS option provides safe, idempotent enum modifications in scripts and migrations
- Renaming enum values updates all references to the old value name throughout the database
- Adding values to enums used in indexed columns may require careful consideration of index maintenance
- The neighbor-based positioning system allows inserting values at any position within the existing enum ordering
- PostgreSQL does not support removing enum values due to the complexity of handling existing data