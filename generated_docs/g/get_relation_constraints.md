# get_relation_constraints

## Location
[src/backend/optimizer/util/plancat.c:1267-1386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1267-L1386)

## Overview
Retrieves and processes all applicable constraint expressions for a given relation, including check constraints, NOT NULL constraints, and partition constraints.

## Definition

```c
static List *
get_relation_constraints(PlannerInfo *root,
						 Oid relationObjectId, RelOptInfo *rel,
						 bool include_noinherit,
						 bool include_notnull,
						 bool include_partition)
```
## Detailed Description
The  function extracts constraint expressions from a relation and transforms them into a standardized format suitable for query optimization. It processes three types of constraints: check constraints, NOT NULL constraints, and partitioning constraints.

For check constraints, the function validates each constraint (skipping unvalidated ones), converts the stored binary representation to expression trees using , and applies canonicalization and constant simplification. The expressions are normalized to use the correct varno for easy comparison with WHERE clause expressions.

For NOT NULL constraints, when requested, the function generates explicit "IS NOT NULL" expressions for each non-dropped attribute marked as . For partition constraints, it includes the partitioning constraints if the relation is a partition.

All constraint expressions undergo the same preprocessing as qual clauses in  to ensure proper matching during query optimization.

## Parameters / Member Variables
- `*root`: PlannerInfo context containing planner state information
- `relationObjectId`: OID of the relation to extract constraints from
- `*rel`: RelOptInfo structure representing the relation in the optimizer
- `include_noinherit`: Whether to include constraints marked NO INHERIT
- `include_notnull`: Whether to generate explicit NOT NULL constraint expressions
- `include_partition`: Whether to include partitioning constraints for partitioned tables
## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](../T/TupleConstr.md)
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [canonicalize_qual](../c/canonicalize_qual.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [list_concat](../l/list_concat.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [NullTest](../N/NullTest.md)
  - [makeVar](../m/makeVar.md)
  - [set_baserel_partition_constraint](../s/set_baserel_partition_constraint.md)
- Called from (representative examples):
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md)

## Notes and Other Information
- This is a static function, not part of the external API
- Assumes the relation is already safely locked by the caller
- Currently invoked at most once per relation per planner run for performance
- Skips unvalidated check constraints for correctness
- Converts expressions to implicit-AND format (List) for easier processing
- For composite columns, argisrow=false is used since attnotnull represents IS DISTINCT FROM NULL rather than SQL-spec IS NOT NULL
- The function handles varno adjustment to ensure constraint expressions reference the correct relation

## Simplified Source

```c
static List *
get_relation_constraints(PlannerInfo *root,
                        Oid relationObjectId, RelOptInfo *rel,
                        bool include_noinherit,
                        bool include_notnull,
                        bool include_partition)
{
    List *result = NIL;
    Index varno = rel->relid;
    Relation relation;
    TupleConstr *constr;

    // Open the relation (assumed already locked)
    relation = table_open(relationObjectId, NoLock);

    constr = relation->rd_att->constr;
    if (constr != NULL)
    {
        // Process check constraints
        for (int i = 0; i < constr->num_check; i++)
        {
            // Skip invalid or no-inherit constraints if not wanted
            if (!constr->check[i].ccvalid ||
                (constr->check[i].ccnoinherit && !include_noinherit))
                continue;

            // Parse and canonicalize constraint expression
            Node *cexpr = stringToNode(constr->check[i].ccbin);
            cexpr = eval_const_expressions(root, cexpr);
            cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);

            // Adjust variable references
            if (varno != 1)
                ChangeVarNodes(cexpr, 1, varno, 0);

            // Add to result list
            result = list_concat(result, make_ands_implicit((Expr *) cexpr));
        }

        // Add NOT NULL constraints if requested
        if (include_notnull && constr->has_not_null)
        {
            int natts = relation->rd_att->natts;
            for (int i = 1; i <= natts; i++)
            {
                Form_pg_attribute att = TupleDescAttr(relation->rd_att, i - 1);
                if (att->attnotnull && !att->attisdropped)
                {
                    // Create IS NOT NULL test
                    NullTest *ntest = makeNode(NullTest);
                    ntest->arg = (Expr *) makeVar(varno, i, att->atttypid,
                                                  att->atttypmod, att->attcollation, 0);
                    ntest->nulltesttype = IS_NOT_NULL;
                    ntest->argisrow = false;
                    ntest->location = -1;
                    result = lappend(result, ntest);
                }
            }
        }
    }

    // Add partition constraints if requested
    if (include_partition && relation->rd_rel->relispartition)
    {
        set_baserel_partition_constraint(relation, rel);
        result = list_concat(result, rel->partition_qual);
    }

    table_close(relation, NoLock);
    return result;
}
```