# FigureColnameInternal

## Location
[src/backend/parser/parse_target.c:1743-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1743-L2033)

## Overview
FigureColnameInternal is the internal workhorse function for FigureColname that determines appropriate column names for SQL expressions by analyzing parse tree nodes and returning a confidence level for the chosen name.

## Definition

```c
structor:
			/* make JSON_OBJECT act like a regular function */
			*name = "json_object";
```
## Detailed Description
This function recursively traverses PostgreSQL parse tree nodes to extract meaningful column names from various SQL expression types. It implements a confidence-based naming system where different node types and naming contexts yield different confidence levels:

- **0**: No information available
- **1**: Second-best name choice (fallback options)  
- **2**: Good name choice (preferred options)

The function handles a comprehensive set of SQL expression types including column references, function calls, type casts, subqueries, case expressions, XML functions, JSON functions, and SQL value functions. For complex expressions, it attempts to find the most meaningful identifier, often recursing into sub-expressions when direct naming isn't available.

## Parameters / Member Variables
- : The parse tree node to analyze for column name extraction
- : Output parameter - pointer to char pointer that will be set to the chosen column name if a suitable name is found (only modified when return value > 0)

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (for node type identification)
  - strVal (for extracting string values)
  - llast (for getting last list element)
  - linitial (for getting first list element)
  - elog (for error reporting)
  - IsA (for type checking)
  - lfirst (for list iteration)
  - Various node type constants (T_ColumnRef, T_FuncCall, etc.)
  - Various enum constants (AEXPR_NULLIF, SVFOP_*, IS_*, JSON_*_OP)

- Called from (representative examples):
  - [FigureColname](FigureColname.md) (main public interface)
  - [FigureIndexColname](FigureIndexColname.md) (for index column naming)
  - [FigureColnameInternal](FigureColnameInternal.md) (recursive self-calls for complex expressions)

## Notes and Other Information
- This is a static function, only accessible within src/backend/parser/parse_target.c
- The function uses a large switch statement to handle different node types, with each case implementing specific logic for extracting meaningful names
- For function calls, it uses the function name as the column name
- For column references and indirections, it extracts the rightmost field name
- Special handling exists for SQL standard functions (NULLIF, GROUPING, MERGE_ACTION) and built-in value functions (CURRENT_DATE, etc.)
- JSON and XML functions are given descriptive names based on their operation type
- The confidence scoring system allows callers to prefer higher-confidence naming choices
- Recursive calls are used for wrapped expressions like TypeCast and CollateClause to unwrap and find the underlying meaningful name

## Simplified Source

```c
static int
FigureColnameInternal(Node *node, char **name)
{
    int strength = 0;

    if (node == NULL)
        return strength;

    switch (nodeTag(node))
    {
        case T_ColumnRef:
            // Extract last field name from column reference
            foreach(l, ((ColumnRef *) node)->fields) {
                if (IsA(lfirst(l), String))
                    fname = strVal(lfirst(l));
            }
            if (fname) {
                *name = fname;
                return 2;  // Good confidence
            }
            break;

        case T_A_Indirection:
            // Handle array/field access - get last field name
            foreach(l, ((A_Indirection *) node)->indirection) {
                if (IsA(lfirst(l), String))
                    fname = strVal(lfirst(l));
            }
            if (fname) {
                *name = fname;
                return 2;
            }
            // Recurse into base expression
            return FigureColnameInternal(((A_Indirection *) node)->arg, name);

        case T_FuncCall:
            // Use function name as column name
            *name = strVal(llast(((FuncCall *) node)->funcname));
            return 2;

        case T_TypeCast:
            // Try to get name from casted expression first
            strength = FigureColnameInternal(((TypeCast *) node)->arg, name);
            if (strength <= 1 && ((TypeCast *) node)->typeName != NULL) {
                // Fall back to type name
                *name = strVal(llast(((TypeCast *) node)->typeName->names));
                return 1;  // Lower confidence
            }
            break;

        case T_SubLink:
            // Handle subqueries
            switch (((SubLink *) node)->subLinkType) {
                case EXISTS_SUBLINK:
                    *name = "exists";
                    return 2;
                case ARRAY_SUBLINK:
                    *name = "array";
                    return 2;
                case EXPR_SUBLINK:
                    // Get column name from subquery's target
                    if (IsA(query, Query)) {
                        TargetEntry *te = (TargetEntry *) linitial(query->targetList);
                        if (te->resname) {
                            *name = te->resname;
                            return 2;
                        }
                    }
                    break;
            }
            break;

        // Built-in function-like expressions
        case T_CoalesceExpr:
            *name = "coalesce";
            return 2;
        case T_A_ArrayExpr:
            *name = "array";
            return 2;
        case T_RowExpr:
            *name = "row";
            return 2;
        case T_GroupingFunc:
            *name = "grouping";
            return 2;

        // SQL value functions (CURRENT_DATE, etc.)
        case T_SQLValueFunction:
            switch (((SQLValueFunction *) node)->op) {
                case SVFOP_CURRENT_DATE:
                    *name = "current_date";
                    return 2;
                case SVFOP_CURRENT_TIMESTAMP:
                case SVFOP_CURRENT_TIMESTAMP_N:
                    *name = "current_timestamp";
                    return 2;
                // ... other SQL value functions
            }
            break;

        // JSON functions
        case T_JsonFuncExpr:
            switch (((JsonFuncExpr *) node)->op) {
                case JSON_EXISTS_OP:
                    *name = "json_exists";
                    return 2;
                case JSON_QUERY_OP:
                    *name = "json_query";
                    return 2;
                case JSON_VALUE_OP:
                    *name = "json_value";
                    return 2;
            }
            break;

        case T_CaseExpr:
            // Try to get name from default result, fall back to "case"
            strength = FigureColnameInternal((Node *) ((CaseExpr *) node)->defresult, name);
            if (strength <= 1) {
                *name = "case";
                return 1;
            }
            break;
    }

    return strength;
}
```