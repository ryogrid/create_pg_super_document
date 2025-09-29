# CheckPredicate

## Location
[src/backend/commands/indexcmds.c:1792-1818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L1792-L1818)

## Overview
Validates that a given partial-index predicate is suitable for use in an index definition, ensuring it contains only immutable functions.

## Definition
```c
static void CheckPredicate(Expr *predicate)
```

## Detailed Description
CheckPredicate is a validation function used during index creation to ensure that partial index predicates meet PostgreSQL's requirements for correctness and consistency. The function performs essential checks to prevent the creation of problematic partial indexes that could lead to inconsistent query results or system instability.

The function originally imposed stricter constraints on predicate forms to ensure compatibility with the index path selection logic in indxpath.c. However, the implementation was relaxed to allow more flexible predicate expressions, recognizing that partial indexes serve broader purposes beyond just query optimization, such as implementing unique constraints across table subsets.

The primary validation performed is ensuring that the predicate contains only immutable functions. This restriction is critical because:
- Mutable functions can return different results for the same inputs over time
- This could cause rows to be included or excluded from the index inconsistently
- Query results could become unpredictable when using the partial index

The function relies on transformExpr() having already rejected inappropriate constructs like subqueries, aggregates, and window functions based on the expression kind for predicates.

## Parameters / Member Variables
- `predicate`: The Expr representing the WHERE clause condition for the partial index

## Dependencies
- Functions called/Symbols referenced:
  - [contain_mutable_functions_after_planning](../c/contain_mutable_functions_after_planning.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)

## Notes and Other Information
- Static function only used within indexcmds.c for internal validation
- Originally had stricter requirements for indxpath.c compatibility but was relaxed for flexibility
- Essential for preventing inconsistent partial index behavior due to mutable functions
- Part of the broader index validation framework during CREATE INDEX operations
- The function assumes transformExpr() has already validated expression structure
- Critical for maintaining MVCC consistency when using partial indexes
- Located in src/backend/commands/indexcmds.c:1792-1818

## Simplified Source

```c
static void CheckPredicate(Expr *predicate) {
    // Check for mutable functions in the predicate
    // Mutable functions could cause inconsistent index behavior
    if (contain_mutable_functions_after_planning(predicate)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("functions in index predicate must be marked IMMUTABLE")));
    }
}
```