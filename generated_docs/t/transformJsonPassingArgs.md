# transformJsonPassingArgs

## Location
[src/backend/parser/parse_expr.c:4637-4662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4637-L4662)

## Overview
Transforms SQL/JSON PASSING clause arguments into lists of expressions and parameter names for JSON function processing.

## Definition
```c
static void transformJsonPassingArgs(ParseState *pstate, const char *constructName,
                                   JsonFormatType format, List *args,
                                   List **passing_values, List **passing_names)
```

## Detailed Description
The transformJsonPassingArgs function processes the PASSING clause arguments used in SQL/JSON functions. The PASSING clause allows SQL expressions to be passed as named parameters to JSON path expressions. This function iterates through the provided arguments, transforms each value expression using transformJsonValueExpr to ensure proper JSON formatting, and builds two parallel lists: one containing the transformed expressions and another containing the parameter names. These lists are used during JSON path evaluation to substitute parameter values.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and state information
- `constructName`: Name of the JSON construct (for error reporting purposes)
- `format`: JsonFormatType specifying the expected JSON format for the arguments
- `args`: List of JsonArgument nodes from the PASSING clause
- `passing_values`: Output parameter - pointer to list that will contain transformed expressions
- `passing_names`: Output parameter - pointer to list that will contain parameter names as strings

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonValueExpr](transformJsonValueExpr.md)
  - castNode
  - lfirst
  - [lappend](../l/lappend.md)
  - [makeString](../m/makeString.md)
  - [JsonArgument](../J/JsonArgument.md)
  - JsonFormatType
- Called from (representative examples):
  - [transformJsonFuncExpr](transformJsonFuncExpr.md)

## Notes and Other Information
This function is essential for SQL/JSON parameter binding functionality. It enables the use of SQL expressions as parameters within JSON path expressions, allowing for dynamic JSON querying. The function maintains the order and correspondence between parameter names and values, which is crucial for correct parameter substitution during JSON path evaluation. Located at src/backend/parser/parse_expr.c:4637-4662.

## Simplified Source

```c
static void
transformJsonPassingArgs(ParseState *pstate, const char *constructName,
                        JsonFormatType format, List *args,
                        List **passing_values, List **passing_names) {
    ListCell *lc;

    // Initialize output lists
    *passing_values = NIL;
    *passing_names = NIL;

    // Process each argument in the PASSING clause
    foreach(lc, args) {
        JsonArgument *arg = castNode(JsonArgument, lfirst(lc));

        // Transform the value expression for JSON format
        Node *expr = transformJsonValueExpr(pstate, constructName,
                                          arg->val, format,
                                          InvalidOid, true);

        // Add transformed expression and parameter name to lists
        *passing_values = lappend(*passing_values, expr);
        *passing_names = lappend(*passing_names, makeString(arg->name));
    }
}
```