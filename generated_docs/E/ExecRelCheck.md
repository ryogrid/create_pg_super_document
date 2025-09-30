# ExecRelCheck

## Location
[src/backend/executor/execMain.c:1719-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1719-L1793)

## Overview
ExecRelCheck validates that a tuple meets all check constraints defined for a result relation, returning NULL if all constraints pass or the name of the first failed constraint.

## Definition
```c
static const char *ExecRelCheck(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate)
```

## Detailed Description
ExecRelCheck is responsible for enforcing check constraints on tuples being inserted or updated in a result relation. The function performs comprehensive constraint validation through several key phases:

1. **Constraint Count Verification**: Validates that the number of check constraints in the relation descriptor matches the expected count, failing with an error if there's a mismatch (indicating missing constraint records).

2. **Expression Preparation**: On first execution for a result relation, parses and prepares all constraint expressions from their string representation into executable ExprState objects, storing them in the per-query memory context for reuse.

3. **Constraint Evaluation**: Evaluates each prepared constraint expression against the tuple using the per-tuple expression context, following SQL semantics where NULL constraint results are treated as success.

The function implements lazy initialization of constraint expressions for performance efficiency and uses appropriate memory contexts to ensure expressions persist throughout query execution while per-tuple evaluation contexts are properly managed.

## Parameters / Member Variables
- `resultRelInfo`: Pointer to ResultRelInfo containing relation descriptor and cached constraint expressions (`ri_ConstraintExprs`)
- `slot`: TupleTableSlot containing the tuple to be validated against the constraints
- `estate`: Pointer to EState providing execution context, query memory context, and per-tuple expression context

## Dependencies
- Functions called/Symbols referenced:
  - [stringToNode](../s/stringToNode.md) (parses constraint string representations into expression trees)
  - [ExecPrepareExpr](ExecPrepareExpr.md) (prepares expressions for execution)
  - GetPerTupleExprContext (obtains per-tuple expression evaluation context)
  - [ExecCheck](ExecCheck.md) (evaluates constraint expressions with proper NULL handling)
  - [ConstrCheck](../C/ConstrCheck.md) (constraint structure type)
- Called from:
  - [ExecConstraints](ExecConstraints.md) (main constraint enforcement function)

## Notes and Other Information
- This is a static function accessible only within execMain.c
- Follows SQL standard semantics where NULL constraint evaluation results are treated as constraint satisfaction
- Uses lazy initialization pattern: constraint expressions are prepared only on first use and cached for subsequent evaluations
- Critical error handling: fails immediately if constraint metadata is inconsistent between relation descriptor and system catalogs
- Memory context management ensures constraint expressions persist in query context while evaluation uses per-tuple context
- The function returns the name of the first failed constraint, allowing callers to provide specific error messages
- Part of PostgreSQL's constraint enforcement infrastructure, typically called during INSERT and UPDATE operations
- Efficient design: avoids re-parsing constraint expressions on every tuple by caching prepared expressions

## Simplified Source

```c
static const char *ExecRelCheck(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate) {
    Relation rel = resultRelInfo->ri_RelationDesc;
    int ncheck = rel->rd_att->constr->num_check;
    ConstrCheck *check = rel->rd_att->constr->check;
    ExprContext *econtext;
    MemoryContext oldContext;

    // Verify constraint count matches catalog
    if (ncheck != rel->rd_rel->relchecks)
        elog(ERROR, "%d pg_constraint record(s) missing for relation \"%s\"",
             rel->rd_rel->relchecks - ncheck, RelationGetRelationName(rel));

    // Prepare constraint expressions on first call (lazy initialization)
    if (resultRelInfo->ri_ConstraintExprs == NULL) {
        oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
        resultRelInfo->ri_ConstraintExprs = (ExprState **) palloc(ncheck * sizeof(ExprState *));

        for (int i = 0; i < ncheck; i++) {
            Expr *checkconstr = stringToNode(check[i].ccbin);
            resultRelInfo->ri_ConstraintExprs[i] = ExecPrepareExpr(checkconstr, estate);
        }
        MemoryContextSwitchTo(oldContext);
    }

    // Set up expression evaluation context
    econtext = GetPerTupleExprContext(estate);
    econtext->ecxt_scantuple = slot;

    // Evaluate each constraint expression
    for (int i = 0; i < ncheck; i++) {
        ExprState *checkconstr = resultRelInfo->ri_ConstraintExprs[i];

        // SQL semantics: NULL constraint result is treated as success
        if (!ExecCheck(checkconstr, econtext))
            return check[i].ccname;  // Return name of failed constraint
    }

    return NULL;  // All constraints passed
}
```