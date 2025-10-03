# simplify_EXISTS_query

## Location
[src/backend/optimizer/plan/subselect.c:1540-1627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1540-L1627)

## Overview
Removes unnecessary SQL features from an EXISTS subquery that don't affect whether it returns zero or more than zero rows, primarily simplifying the target list and various clauses to optimize query execution.

## Definition

```c
static bool
simplify_EXISTS_query(PlannerInfo *root, Query *query)
```
## Detailed Description
This function optimizes EXISTS subqueries by removing SQL constructs that don't affect the fundamental question that EXISTS answers: whether any rows are returned. Since EXISTS only cares about row existence (not row contents), many SQL features can be safely eliminated.

The function performs the following optimizations:
- Removes the target list (SELECT clause) entirely, as column values are irrelevant for EXISTS
- Eliminates GROUP BY, WINDOW, DISTINCT, and ORDER BY clauses since they don't change whether rows exist
- Handles LIMIT clauses specially - removes positive constant LIMIT values (like LIMIT 1) but rejects complex LIMIT expressions
- Removes various flags like hasDistinctOn

The function includes safety checks and will not simplify queries that use complex features like set operations, aggregates, grouping sets, window functions, set-returning functions, modifying CTEs, HAVING clauses, OFFSET clauses, or row-level locking, as their effects on row existence are complex.

## Parameters / Member Variables
- : PlannerInfo structure containing the planning context, used for constant expression evaluation
- : The Query node representing the EXISTS subquery to be simplified

## Dependencies
- Functions called/Symbols referenced:
  - : Evaluates and simplifies constant expressions in LIMIT clauses
  - : Enumeration value to check if query is a SELECT statement
  - : Extracts int64 value from a Datum for LIMIT validation
- Called from (representative examples):
  - : Uses this function to prepare EXISTS subqueries for join conversion
  - : Calls this function when creating subplans for EXISTS subqueries

## Notes and Other Information
- Returns true if the target list was successfully discarded, false otherwise
- The function may cause behavioral changes by suppressing errors or side effects from volatile functions in the target list, but this is considered acceptable for typical usage
- Specifically handles the common pattern of "SELECT * FROM ... LIMIT 1" in EXISTS clauses by recognizing and optimizing it
- The LIMIT clause evaluation uses eval_const_expressions to handle cases like "LIMIT int8(1::int4)" which appear after parsing
- Part of PostgreSQL's EXISTS optimization strategy that enables better join planning and execution

## Simplified Source

```c
static bool
simplify_EXISTS_query(PlannerInfo *root, Query *query)
{
    // Don't simplify complex queries with set operations, aggregates,
    // grouping sets, window functions, SRFs, modifying CTEs, HAVING,
    // OFFSET, or row locking
    if (query->commandType != CMD_SELECT ||
        query->setOperations ||
        query->hasAggs ||
        query->groupingSets ||
        query->hasWindowFuncs ||
        query->hasTargetSRFs ||
        query->hasModifyingCTE ||
        query->havingQual ||
        query->limitOffset ||
        query->rowMarks)
        return false;

    // Handle LIMIT clauses - allow positive constants like LIMIT 1
    if (query->limitCount)
    {
        // Evaluate constant expressions in LIMIT clause
        Node *node = eval_const_expressions(root, query->limitCount);
        query->limitCount = node;

        // Only accept constant LIMIT values
        if (!IsA(node, Const))
            return false;

        Const *limit = (Const *) node;
        Assert(limit->consttype == INT8OID);

        // Reject zero or negative LIMIT
        if (!limit->constisnull && DatumGetInt64(limit->constvalue) <= 0)
            return false;

        // Remove the LIMIT clause - it doesn't affect EXISTS semantics
        query->limitCount = NULL;
    }

    // Remove all clauses that don't affect row existence:
    // - Target list (column values don't matter for EXISTS)
    // - GROUP BY, WINDOW, DISTINCT, ORDER BY (don't change whether rows exist)
    query->targetList = NIL;
    query->groupClause = NIL;
    query->windowClause = NIL;
    query->distinctClause = NIL;
    query->sortClause = NIL;
    query->hasDistinctOn = false;

    return true;  // Successfully simplified
}
```