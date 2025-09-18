# pull_varattnos_context

## Location
[src/backend/optimizer/util/var.c:43-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L43-L48)

## Overview
A context structure used by the pull_varattnos functionality to collect attribute numbers from Var nodes of a specific relation during expression tree traversal.

## Definition
```c
typedef struct
{
    Bitmapset  *varattnos;
    Index       varno;
} pull_varattnos_context;
```

## Detailed Description
The pull_varattnos_context structure serves as a walker context for the pull_varattnos_walker function, which traverses expression trees to identify all column attribute numbers referenced by Var nodes that belong to a specific relation. This is used during query planning to determine which columns of a particular table are actually accessed by an expression, enabling optimizations such as projection pruning. The context filters Var nodes by relation ID and collects the attribute numbers of matching columns.

## Parameters / Member Variables
- `varattnos`: A Bitmapset containing the collected attribute numbers from Var nodes matching the target relation
- `varno`: The target relation ID (Index) for which to collect attribute numbers

## Dependencies
- Functions called/Symbols referenced:
  - [Bitmapset](../B/Bitmapset.md) (typedef)
  - Index (typedef)
- Called from (representative examples):
  - [pull_varattnos](pull_varattnos.md)
  - [pull_varattnos_walker](pull_varattnos_walker.md)
  - flatten_join_alias_vars_context

## Notes and Other Information
This context structure is specifically designed to extract column usage information for a single relation. The walker function only processes Var nodes with varlevelsup == 0 (current query level) and matching varno, adding their attribute numbers to the bitmap after adjusting for FirstLowInvalidHeapAttributeNumber. This functionality is essential for determining the minimal set of columns needed from a relation during query execution planning.