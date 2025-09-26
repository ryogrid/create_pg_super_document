# find_jointree_node_for_rel

## Location
[src/backend/optimizer/prep/prepjointree.c:4159-4207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4159-L4207)

## Overview
Recursively searches through a join tree structure to locate the specific node that corresponds to a given base relation or join relation table index.

## Definition
```c
static Node *find_jointree_node_for_rel(Node *jtnode, int relid)
```

## Detailed Description
This static function performs a depth-first recursive search through PostgreSQL's join tree structure to locate a specific node identified by its range table index. The function handles the three main types of join tree nodes and searches through them systematically:

1. **RangeTblRef nodes**: Compares the target relid directly with the node's rtindex
2. **FromExpr nodes**: Recursively searches through all elements in the fromlist
3. **JoinExpr nodes**: First checks if the join itself matches the relid, then recursively searches both left and right arguments

The search strategy is designed to find the first matching node in the tree traversal order. For JoinExpr nodes, the function checks the join's own rtindex before descending into its children, ensuring that join nodes themselves can be found when searched for.

The function returns the first matching Node pointer found, or NULL if no matching relation ID exists in the subtree.

## Parameters / Member Variables
- `jtnode`: The join tree node to search within (can be RangeTblRef, FromExpr, or JoinExpr)
- `relid`: The range table index of the relation or join to find

## Dependencies
- Functions called/Symbols referenced:
  - find_jointree_node_for_rel (recursive)
  - nodeTag
  - RangeTblRef
  - FromExpr 
  - JoinExpr
- Called from (representative examples):
  - get_relids_for_join
  - find_jointree_node_for_rel (recursive calls)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the prepjointree.c file
- Returns NULL if the input node is NULL or if the target relid is not found
- Uses depth-first traversal, returning the first match encountered
- For JoinExpr nodes, checks the join's own rtindex before checking its children, allowing joins themselves to be located
- Raises an ERROR for unrecognized node types, indicating internal corruption or programming errors
- The function is essential for operations that need to locate specific parts of the join tree for analysis or modification