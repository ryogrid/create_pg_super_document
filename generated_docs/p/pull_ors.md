# pull_ors

## Location
[src/backend/optimizer/prep/prepqual.c:349-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L349-L405)

## Overview
Recursively flattens nested OR clauses into a single flat list of OR arguments, eliminating unnecessary nesting in boolean expressions.

## Definition

```c
structure is searched.
 *
 * While at it, we remove any NULL constants within the top-level AND/OR
 * structure, eg in a WHERE clause, "x OR NULL::boolean" is reduced to "x".
 * In general that would change the result, so eval_const_expressions can't
 * do it;
```
## Detailed Description
The  function takes the argument list of an OR clause and recursively flattens any nested OR clauses within it. This transformation converts deeply nested OR structures like  into a flat list  that can be processed more efficiently.

The function iterates through each argument in the input OR list. When it encounters a nested OR clause (detected using ), it recursively calls itself to flatten that subclause and concatenates the results. Non-OR expressions are simply appended to the output list as-is.

This flattening is the OR counterpart to  and serves similar purposes:
- Simplifies the structure for further processing
- Eliminates unnecessary intermediate nodes
- Makes it easier to apply other optimizations like duplicate removal
- Reduces the depth of expression trees

The original input list structure is preserved (not modified), and a new flattened list is returned.

## Parameters / Member Variables
- : The argument list of an OR clause to be flattened (List of Node pointers)

## Dependencies
- Functions called/Symbols referenced:
  -  - checks if a node is an OR boolean expression
  -  - concatenates two lists together
  -  - recursive call to handle nested OR clauses
  -  - boolean expression node type for accessing args
- Called from (representative examples):
  -  (recursive call) (src/backend/optimizer/prep/prepqual.c:360)
  -  (src/backend/optimizer/prep/prepqual.c:447)
  -  (src/backend/optimizer/prep/prepqual.c:665)

## Notes and Other Information
- This is a static function, only used within the prepqual.c module
- The function preserves the original input list structure and returns a new list
- Essential for boolean expression normalization in PostgreSQL's query optimizer
- Works recursively to handle arbitrarily deep nesting of OR clauses
- Companion function to  for handling AND clause flattening
- The flattening helps subsequent optimization passes work more effectively on simplified structures
- Particularly important for duplicate detection and removal in OR expressions

## Simplified Source

```c
static List *pull_ors(List *orlist) {
    List *out_list = NIL;
    ListCell *arg;

    // Iterate through each argument in the OR list
    foreach(arg, orlist) {
        Node *subexpr = (Node *) lfirst(arg);

        // If it's a nested OR clause, recursively flatten it
        if (is_orclause(subexpr))
            out_list = list_concat(out_list,
                                  pull_ors(((BoolExpr *) subexpr)->args));
        else
            // Otherwise, just add the expression to the output
            out_list = lappend(out_list, subexpr);
    }

    return out_list;
}
```