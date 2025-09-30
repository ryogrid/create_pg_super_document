# find_forced_null_var

## Location
[src/backend/optimizer/util/clauses.c:1977-2025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1977-L2025)

## Overview
Returns the specific variable that is forced to be NULL by a given clause, or NULL if the clause is not a simple IS NULL-type test.

## Definition
Var *find_forced_null_var(Node *node)

## Detailed Description
This function examines individual clauses to identify cases where exactly one variable is constrained to be NULL, without any other conditions. It represents the single-clause case of find_forced_null_vars() but does not handle AND conditions, making it more restrictive but also more precise for certain use cases.

The function specifically looks for two types of expressions:
1. **NullTest expressions**: Direct "var IS NULL" tests where the variable is not a row type
2. **BooleanTest expressions**: "var IS UNKNOWN" tests, which are logically equivalent to "var IS NULL"

The function is used by initsplan.c on individual qualification clauses where it's important to identify precisely which variable is being tested for nullness. This precision is necessary because if an AND combination of an IS NULL clause with other conditions were to survive flattening, the broader find_forced_null_vars() might cause initsplan.c to incorrectly discard the entire clause when only the IS NULL part has been proved redundant.

The function only considers level-zero variables (varlevelsup == 0) to ensure it's dealing with variables from the current query level rather than outer query references.

## Parameters / Member Variables
- node: The expression node to examine for a forced-null variable constraint

## Dependencies
- Functions called/Symbols referenced:
  - [NullTest](../N/NullTest.md) (node type checking)
  - [BooleanTest](../B/BooleanTest.md) (node type checking)
  - IS_NULL (null test type)
  - IS_UNKNOWN (boolean test type)
- Called from (representative examples):
  - [check_redundant_nullability_qual](../c/check_redundant_nullability_qual.md)
  - [find_forced_null_vars](find_forced_null_vars.md)
  - [WindowFuncLists](../W/WindowFuncLists.md)

## Notes and Other Information
- This function is more restrictive than find_forced_null_vars as it only handles single clauses
- It explicitly avoids handling AND conditions to maintain precision for initsplan.c usage
- The function treats "IS UNKNOWN" and "IS NULL" as equivalent for variable nullness constraints
- Only considers current-level variables (varlevelsup == 0) to avoid confusion with outer query references
- Returns NULL if the clause doesn't match the specific patterns it recognizes

## Simplified Source

```c
Var *
find_forced_null_var(Node *node)
{
    if (node == NULL)
        return NULL;

    // Check for "var IS NULL" expressions
    if (IsA(node, NullTest)) {
        NullTest *expr = (NullTest *) node;

        if (expr->nulltesttype == IS_NULL && !expr->argisrow) {
            Var *var = (Var *) expr->arg;

            // Return var if it's a current-level variable
            if (var && IsA(var, Var) && var->varlevelsup == 0)
                return var;
        }
    }
    // Check for "var IS UNKNOWN" expressions (equivalent to IS NULL)
    else if (IsA(node, BooleanTest)) {
        BooleanTest *expr = (BooleanTest *) node;

        if (expr->booltesttype == IS_UNKNOWN) {
            Var *var = (Var *) expr->arg;

            // Return var if it's a current-level variable
            if (var && IsA(var, Var) && var->varlevelsup == 0)
                return var;
        }
    }

    return NULL;
}
```