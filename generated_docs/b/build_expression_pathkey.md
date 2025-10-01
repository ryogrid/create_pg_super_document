# build_expression_pathkey

## Location
[src/backend/optimizer/path/pathkeys.c:998-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L998-L1051)

## Overview
Builds a pathkeys list that describes an ordering by a single expression using a given sort operator, with default sort order assumptions.

## Definition

```c
struct a mergejoin using DESC order rather than ASC order;
```
## Detailed Description
This function creates a single-element pathkeys list for ordering by a specific expression and sort operator. It serves as a convenience wrapper around make_pathkey_from_sortinfo that automatically determines the operator family, input type, and sort direction from the provided sort operator.

The function looks up the operator's properties in the system catalogs to determine the operator family and strategy number, then uses this information to construct appropriate pathkey parameters. It assumes default sort behavior based on the operator's strategy (treating BTGreaterStrategyNumber as descending order).

If the expression is not already part of an EquivalenceClass and create_it is false, the function returns NIL rather than creating new equivalence relationships.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and equivalence classes
- : Expression to create the pathkey for
- : OID of the sort operator to use for ordering
- : Relids representing the relations that the expression can contain variables from
- : Whether to create new equivalence classes if the expression isn't already in one

## Dependencies
- Functions called/Symbols referenced:
  - [get_ordering_op_properties](../g/get_ordering_op_properties.md) (to extract operator family and strategy information)
  - [make_pathkey_from_sortinfo](../m/make_pathkey_from_sortinfo.md) (to create the actual pathkey)
  - [exprCollation](../e/exprCollation.md) (to determine expression's collation)
  - BTGreaterStrategyNumber (constant for determining sort direction)
- Called from (representative examples):
  - [set_function_pathlist](../s/set_function_pathlist.md)

## Notes and Other Information
- This is a convenience function that simplifies pathkey creation for single expressions
- Automatically derives sort direction from operator strategy number
- Assumes that the provided operator is a valid B-tree ordering operator
- Returns a single-element list containing one PathKey, or NIL if pathkey creation fails
- Part of PostgreSQL's query optimization pathkey system for representing sort orders

## Simplified Source

```c
List *
build_expression_pathkey(PlannerInfo *root,
                        Expr *expr,
                        Oid opno,
                        Relids rel,
                        bool create_it)
{
    List *pathkeys;
    Oid opfamily, opcintype;
    int16 strategy;
    PathKey *cpathkey;

    // Look up operator properties in pg_amop
    if (!get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy))
        elog(ERROR, "operator %u is not a valid ordering operator", opno);

    // Create pathkey from sort information
    cpathkey = make_pathkey_from_sortinfo(root,
                                          expr,
                                          opfamily,
                                          opcintype,
                                          exprCollation((Node *) expr),
                                          (strategy == BTGreaterStrategyNumber),
                                          (strategy == BTGreaterStrategyNumber),
                                          0,
                                          rel,
                                          create_it);

    // Return single-element list or NIL
    if (cpathkey)
        pathkeys = list_make1(cpathkey);
    else
        pathkeys = NIL;

    return pathkeys;
}
```