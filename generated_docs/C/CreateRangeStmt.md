# CreateRangeStmt

## Location
[src/include/nodes/parsenodes.h:3707-3712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3707-L3712)

## Overview
CreateRangeStmt represents a CREATE TYPE statement for defining range types in PostgreSQL's parse tree structure.

## Definition
```c
typedef struct CreateRangeStmt
{
    NodeTag     type;
    List       *typeName;       /* qualified name (list of String) */
    List       *params;         /* range parameters (list of DefElem) */
} CreateRangeStmt;
```

## Detailed Description
CreateRangeStmt is a parse tree node that represents CREATE TYPE statements used to define range types in PostgreSQL. Range types are data types representing ranges of values of some element type (called the range's subtype). For example, ranges of timestamp might be used to represent the ranges of time that a meeting room is reserved. Range types are useful for representing intervals, periods, or any contiguous set of values where you need to express concepts like "between X and Y" efficiently.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateRangeStmt node
- `typeName`: List of String nodes representing the qualified name of the range type (e.g., schema.type_name)
- `params`: List of DefElem nodes containing range type parameters such as subtype, canonical function, difference function, etc.

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [List](../L/List.md) (containing String and DefElem nodes)
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- [Range](../R/Range.md) types support various operators for containment, overlap, intersection, and ordering
- The params list can include subtype (required), canonical function, subtype difference function, and collation specifications
- Common built-in range types include int4range, int8range, numrange, tsrange, tstzrange, and daterange
- [Range](../R/Range.md) types support both inclusive and exclusive bounds using bracket notation (e.g., "[1,10)" means 1 ≤ x < 10)
- The canonical function normalizes equivalent range representations
- The difference function enables more efficient GiST indexing by quantifying the "distance" between range bounds
- [Range](../R/Range.md) types are particularly useful for temporal data, reservation systems, and any application dealing with continuous intervals