# pull_up_simple_union_all

## Location
src/backend/optimizer/prep/prepjointree.c: 1469 - 1550

## Overview
Transforms a simple UNION ALL subquery into an append relation by pulling up its leaf subqueries and creating AppendRelInfo nodes for efficient execution.

## Definition


## Detailed Description
This function handles the optimization of simple UNION ALL subqueries by converting them into PostgreSQL's "append relation" structure. Rather than executing the UNION operation at runtime, the planner can treat the UNION ALL as a logical append operation over multiple relations, which is more efficient.

The transformation process involves:

1. **Range Table Processing**: Makes a modifiable copy of the subquery's range table and adjusts variable sublevels since the leaf queries will be one level closer to their parent after pull-up.

2. **LATERAL Propagation**: If the UNION ALL subquery was marked as LATERAL, propagates this marker to all child RTEs to ensure later planning stages check for lateral cross-references.

3. **Range Table Merging**: Combines the child range tables and permission info with the parent query's range table.

4. **Append Relation Creation**: Recursively processes the setOperations tree to create AppendRelInfo nodes for each leaf subquery, which allows the executor to scan them as a unified append relation.

5. **Inheritance Marking**: Marks the parent RTE with the inheritance flag () to indicate it represents an append relation.

Unlike other pull-up operations, this function doesn't modify the jointree structure itself, as the original RangeTblRef continues to represent the append relation.

## Parameters / Member Variables
- : PlannerInfo structure for the parent query
- : RangeTblRef node representing the UNION ALL subquery
- : RangeTblEntry for the UNION ALL subquery being converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates modifiable copy of subquery's range table
  -  - Adjusts variable sublevels in range table
  -  - Merges range tables and permission info
  -  - Recursively processes UNION leaf queries
  -  - [Node](../N/Node.md) type for range table references
  -  - Range table entry type for subqueries

- Called from (representative examples):
  -  - During recursive subquery processing

## Notes and Other Information
- This optimization is specifically for UNION ALL operations; UNION (with duplicate elimination) cannot be optimized this way
- The function assumes that  has already validated that the subquery is eligible for this transformation
- [Variable](../V/Variable.md) offset adjustment is not needed because UNION leaf queries cannot cross-reference each other
- The resulting append relation allows the executor to use more efficient scanning strategies
- LATERAL handling ensures that any potential lateral cross-references in leaf queries are properly marked for later validation
- The inheritance flag () is the key marker that tells the planner this RTE represents an append relation
- Unlike simple subquery pull-up, the jointree structure remains unchanged - only the RTE semantics change