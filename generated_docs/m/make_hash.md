# make_hash

## Location
[src/backend/optimizer/plan/createplan.c:6005-6027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6005-L6027)

## Overview
Creates a Hash plan node that builds a hash table for use by a HashJoin operation in PostgreSQL's query execution plan tree.

## Definition
```c
static Hash *make_hash(Plan *lefttree, List *hashkeys, Oid skewTable, AttrNumber skewColumn, bool skewInherit)
```

## Detailed Description
The `make_hash` function constructs a Hash plan node, which is responsible for building the hash table used in hash join operations. This node is typically the right child of a HashJoin node and processes the inner relation by building an in-memory hash table based on the join keys. The hash table enables efficient lookups during the join phase when rows from the outer relation are probed against it.

The function also handles skew optimization parameters, which can improve performance when the data distribution in hash keys is highly skewed (i.e., some values appear much more frequently than others).

## Parameters / Member Variables
- `lefttree`: Plan node that will provide the input tuples to be hashed (typically the inner relation of the join)
- `hashkeys`: List of expressions that will be used as hash keys for building the hash table
- `skewTable`: OID of a table whose statistics should be used for skew optimization (InvalidOid if not applicable)
- `skewColumn`: Column number in skewTable for which skew optimization should be applied (0 if not applicable)
- `skewInherit`: Boolean indicating whether to consider inheritance when applying skew optimization

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the Hash node)
  - Hash (plan node structure)
- Called from (representative examples):
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md) (in createplan.c:4878)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the plan creation subsystem
- Hash nodes are always paired with HashJoin nodes, with the Hash node as the right child building the hash table from the inner relation
- The target list is copied from the input plan (lefttree) since the Hash node needs to pass through all columns needed by the parent HashJoin
- The qual field is set to NIL because filtering is typically done at the HashJoin level, not in the Hash node
- Skew optimization helps handle cases where certain hash key values are much more common than others, which can lead to uneven hash bucket distribution
- The hash table built by this node must fit in work_mem, or PostgreSQL will use batch processing to handle larger datasets
- Hash nodes are leaf nodes in the sense that they have no right child, but they process input from their left child