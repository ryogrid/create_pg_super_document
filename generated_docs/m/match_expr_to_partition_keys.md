# match_expr_to_partition_keys

## Location
[src/backend/optimizer/util/relnode.c:2236-2284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L2236-L2284)

## Overview
Attempts to match an expression against the partition keys of a partitioned relation, returning the ordinal position of the matched key or -1 if no match is found.

## Definition

```c
static int
match_expr_to_partition_keys(Expr *expr, RelOptInfo *rel, bool strict_op)
```
## Detailed Description
This function performs expression matching against both nullable and non-nullable partition key expressions of a partitioned relation. It strips away RelabelType nodes (which represent type coercion decorations) from the input expression and then compares it against stored partition key expressions using structural equality.

The function operates in two phases:
1. Always searches non-nullable partition key expressions () for matches
2. If  is true, also searches nullable partition key expressions ()

The distinction between nullable and non-nullable partition keys is crucial for correctness in join operations. Non-nullable partition keys can always be safely used for partitionwise operations. Nullable partition keys can only be used when the join operator is strict (null-rejecting), because strict operators ensure that NULL values will not match across partitions, maintaining partitionwise join correctness.

## Parameters / Member Variables
- : The expression to match against partition keys (typically from a join condition)
- : The partitioned RelOptInfo containing partition key information to match against
- : Boolean flag indicating whether the expression will be used with a strict operator, enabling nullable partition key consideration

## Dependencies
- Functions called/Symbols referenced:
  - : Performs structural equality comparison between expressions
  - : Type cast decoration node that is stripped during matching
  - : Macro for type checking expression nodes
  - : Safe type casting macro with assertion
- Called from (representative examples):
  - : Uses this function twice to match expressions from both sides of a join condition

## Notes and Other Information
- Requires the relation to be partitioned with initialized partition expressions (assertions enforced)
- Strips RelabelType decorations to enable matching of expressions that are semantically equivalent but have different type representations
- Returns the zero-based ordinal position of the matched partition key, enabling caller to verify that both sides of a join condition reference the same partition key position
- The nullable vs non-nullable distinction is essential for maintaining correctness in outer join scenarios where NULL values may be introduced
- Used exclusively in the context of partitionwise join optimization to validate that join conditions properly align with partition boundaries