# generate_subquery_params

## Location
[src/backend/optimizer/plan/subselect.c:580-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L580-L612)

## Overview
Creates a list of Param nodes representing the output columns of a subquery's target list, used for parameter passing between subqueries and outer queries.

## Definition
```c
static List *generate_subquery_params(PlannerInfo *root, List *tlist, List **paramIds)
```

## Detailed Description
This function processes a subquery's target list and generates corresponding Param nodes that will be used to pass values from the subquery to the outer query. Each non-resjunk entry in the target list results in a new PARAM_EXEC parameter that captures the type information (datatype, typmod, and collation) of the corresponding expression.

The function serves a critical role in subplan execution by establishing the parameter interface between subqueries and their parent queries. It ensures that the outer query can access subquery results through properly typed parameters, maintaining type safety and enabling efficient parameter substitution during execution.

The function skips resjunk entries since these are internal working columns that should not be visible to the outer query.

## Parameters / Member Variables
- `root`: PlannerInfo context for the current query level
- `tlist`: Target list of the subquery whose output columns need parameters
- `paramIds`: Output parameter - pointer to list where parameter IDs will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [generate_new_exec_param](generate_new_exec_param.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)
  - [lappend](../l/lappend.md)
  - [lappend_int](../l/lappend_int.md)
- Called from (representative examples):
  - [build_subplan](../b/build_subplan.md) (multiple times for different sublink types)

## Notes and Other Information
- The function is static, meaning it's only accessible within the subselect.c file
- Returns both the list of Param nodes and fills in the paramIds list through the output parameter
- Only processes non-resjunk entries from the target list
- Each generated parameter has the same type information as the corresponding target list expression
- The generated parameters are of type PARAM_EXEC, which are used for inter-plan communication
- Located in src/backend/optimizer/plan/subselect.c:580-612

## Simplified Source

```c
static List *
generate_subquery_params(PlannerInfo *root, List *tlist, List **paramIds)
{
    List *result = NIL;
    List *ids = NIL;
    ListCell *lc;

    // Process each target list entry
    foreach(lc, tlist)
    {
        TargetEntry *tent = (TargetEntry *) lfirst(lc);

        // Skip internal working columns
        if (tent->resjunk)
            continue;

        // Create parameter with same type info as expression
        Param *param = generate_new_exec_param(root,
                                             exprType((Node *) tent->expr),
                                             exprTypmod((Node *) tent->expr),
                                             exprCollation((Node *) tent->expr));

        // Add to result lists
        result = lappend(result, param);
        ids = lappend_int(ids, param->paramid);
    }

    *paramIds = ids;
    return result;
}
```