# lookupVariable

## Location
[src/bin/pgbench/pgbench.c:1604-1630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1604-L1630)

## Overview
Locates a variable by name in a Variables collection using efficient binary search, automatically sorting the variable array if needed and returning NULL if the variable is not found.

## Definition
```c
static Variable *lookupVariable(Variables *variables, char *name)
```

## Detailed Description
The `lookupVariable` function provides efficient variable lookup functionality in pgbench by implementing a lazy-sorted binary search approach. The function first checks if the variables array is already sorted, and if not, it sorts the array using qsort() with the `compareVariableNames` comparator before performing the search. This lazy sorting approach optimizes performance by only sorting when necessary and ensuring that subsequent lookups benefit from the sorted state. The function then uses bsearch() to perform an O(log n) search for the requested variable name. It includes defensive programming to handle edge cases like empty variable arrays, which could cause issues on some platforms. The function returns a pointer to the Variable structure if found, or NULL if the variable doesn't exist.

## Parameters / Member Variables
- `variables`: Pointer to Variables structure containing the variable array and metadata
- `name`: Null-terminated string containing the name of the variable to find

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard C library sorting function)
  - bsearch (standard C library binary search function)
  - [compareVariableNames](../c/compareVariableNames.md) (custom comparator for Variable structures)
- Data types used:
  - [Variables](../V/Variables.md) (structure containing variable array and sorting metadata)
  - [Variable](../V/Variable.md) (individual variable structure with name field)
- Called from (representative examples):
  - [getVariable](../g/getVariable.md) (variable access function)
  - [lookupCreateVariable](lookupCreateVariable.md) (variable creation/lookup function)
  - [evaluateExpr](../e/evaluateExpr.md) (expression evaluation)
  - [main](../m/main.md) (command-line variable processing)

## Notes and Other Information
- Implements lazy sorting strategy - only sorts when vars_sorted flag is false
- Uses binary search for O(log n) lookup performance after initial sort
- Handles edge case of zero variables to prevent core dumps on some Solaris versions
- Maintains sorted state flag to avoid unnecessary re-sorting
- Returns NULL for non-existent variables, allowing callers to handle missing variables appropriately
- Critical component of pgbench's variable management system for efficient lookups
- Located in src/bin/pgbench/pgbench.c:1604-1630 and widely used throughout variable operations

## Simplified Source

```c
static Variable *lookupVariable(Variables *variables, char *name) {
    Variable key;

    // Handle empty variable array (prevents issues on some platforms)
    if (variables->nvars <= 0)
        return NULL;

    // Sort array if not already sorted (lazy sorting)
    if (!variables->vars_sorted) {
        qsort(variables->vars, variables->nvars, sizeof(Variable),
              compareVariableNames);
        variables->vars_sorted = true;
    }

    // Perform binary search for the variable
    key.name = name;
    return (Variable *) bsearch(&key,
                                variables->vars,
                                variables->nvars,
                                sizeof(Variable),
                                compareVariableNames);
}
```