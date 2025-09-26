# get_nullingrels_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:4228-4290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4228-L4290)

## Overview
Recursively traverses the join tree to build nulling relationship information, tracking which outer joins can null each base relation based on join types and tree structure.

## Definition
```c
static void get_nullingrels_recurse(Node *jtnode, Relids upper_nullingrels, nullingrel_info *info)
```

## Detailed Description
This function implements the core recursive logic for building nulling relationship information throughout the join tree. It traverses the tree depth-first, maintaining and propagating sets of nulling relation IDs based on the semantics of different join types.

The function handles three main node types with different nulling behaviors:

**RangeTblRef (leaf relations)**: Stores the accumulated upper_nullingrels directly into the info structure for this base relation.

**FromExpr (relation lists)**: Simply propagates the current upper_nullingrels to all child nodes without modification.

**JoinExpr (join operations)**: Implements join-type-specific nulling logic:
- **JOIN_INNER**: Propagates upper_nullingrels unchanged to both sides (no new nulls introduced)
- **JOIN_LEFT/SEMI/ANTI**: Adds this join's rtindex to nullingrels for the right side only (right side can be nulled)
- **JOIN_FULL**: Adds this join's rtindex to nullingrels for both sides (both sides can be nulled)
- **JOIN_RIGHT**: Adds this join's rtindex to nullingrels for the left side only (left side can be nulled)

The function carefully manages memory by creating copies of the upper_nullingrels bitmap set when modifications are needed, ensuring that the passed-down parameter remains constant at each recursion level.

## Parameters / Member Variables
- `jtnode`: The join tree node to process (RangeTblRef, FromExpr, or JoinExpr)
- `upper_nullingrels`: The set of outer join relids that can null relations at this level (treated as constant)
- `info`: The nullingrel_info structure being populated with results

## Dependencies
- Functions called/Symbols referenced:
  - [bms_copy](../b/bms_copy.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [get_nullingrels_recurse](get_nullingrels_recurse.md) (recursive)
  - nodeTag
  - JOIN_INNER, JOIN_LEFT, JOIN_SEMI, JOIN_ANTI, JOIN_FULL, JOIN_RIGHT
- Called from (representative examples):
  - [get_nullingrels](get_nullingrels.md)
  - [get_nullingrels_recurse](get_nullingrels_recurse.md) (recursive calls)

## Notes and Other Information
- This is a static function accessible only within prepjointree.c
- The upper_nullingrels parameter must be treated as read-only at each level
- Memory management is carefully handled - copied bitmap sets are not explicitly freed because they're referenced by leaf relations
- The function implements the precise semantics of SQL outer joins for nulling behavior
- INNER joins don't introduce new nulls, while outer joins null the non-preserved side(s)
- SEMI and ANTI joins are treated like LEFT joins for nulling purposes
- The function validates join types and node types, raising errors for unrecognized types
- Results are stored directly in the info structure's nullingrels array using 1-based indexing