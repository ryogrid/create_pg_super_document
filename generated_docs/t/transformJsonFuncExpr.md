# transformJsonFuncExpr

## Location
[src/backend/parser/parse_expr.c:4271-4636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4271-L4636)

## Overview
Transforms JSON_VALUE, JSON_QUERY, JSON_EXISTS, and JSON_TABLE function expressions into JsonExpr nodes with comprehensive validation and behavior handling.

## Definition
```c
static Node *transformJsonFuncExpr(ParseState *pstate, JsonFuncExpr *func)
```

## Detailed Description
The transformJsonFuncExpr function is a comprehensive transformation handler for SQL/JSON query functions. It processes four major JSON functions: JSON_EXISTS (returns boolean), JSON_QUERY (returns JSON), JSON_VALUE (returns scalar), and JSON_TABLE (returns table). The function performs extensive validation of ON EMPTY and ON ERROR behavior clauses, ensuring they are appropriate for each specific JSON function type. It handles format specifications, quote behavior, wrapper settings, and type coercion. The function creates a JsonExpr node with all necessary configuration for runtime execution, including path specifications, passing arguments, and return type handling.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and state information
- `func`: JsonFuncExpr node representing the parsed JSON function expression (VALUE/QUERY/EXISTS/TABLE)

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonValueExpr](transformJsonValueExpr.md)
  - [transformExprRecurse](transformExprRecurse.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [transformJsonPassingArgs](transformJsonPassingArgs.md)
  - [transformJsonOutput](transformJsonOutput.md)
  - [transformJsonBehavior](transformJsonBehavior.md)
  - makeNode
  - [get_typtype](../g/get_typtype.md)
  - [DomainHasConstraints](../D/DomainHasConstraints.md)
  - ereport
  - [parser_errposition](../p/parser_errposition.md)
  - [exprLocation](../e/exprLocation.md)
  - [exprType](../e/exprType.md)
  - [format_type_be](../f/format_type_be.md)
  - Various JSON behavior constants (JSON_BEHAVIOR_ERROR, JSON_BEHAVIOR_NULL, etc.)
  - Various JSON format constants (JS_FORMAT_JSONB, JS_FORMAT_DEFAULT, etc.)
  - JSON operation constants (JSON_EXISTS_OP, JSON_QUERY_OP, JSON_VALUE_OP, JSON_TABLE_OP)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
This is one of the most complex transformation functions in PostgreSQL's JSON support, handling multiple function types with different semantics. It includes extensive validation logic for behavior clauses, ensuring that each JSON function only accepts appropriate ON EMPTY/ON ERROR behaviors. The function handles type coercion strategies differently for each operation type and manages format specifications and quote behavior. Located at src/backend/parser/parse_expr.c:4271-4636.

## Simplified Source

```c
static Node *
transformJsonFuncExpr(ParseState *pstate, JsonFuncExpr *func)
{
    JsonExpr *jsexpr;
    Node *path_spec;
    const char *func_name = NULL;
    JsonFormatType default_format;

    // Set function name and default format based on operation type
    switch (func->op)
    {
        case JSON_EXISTS_OP:
            func_name = "JSON_EXISTS";
            default_format = JS_FORMAT_DEFAULT;
            break;
        case JSON_QUERY_OP:
            func_name = "JSON_QUERY";
            default_format = JS_FORMAT_JSONB;
            break;
        case JSON_VALUE_OP:
            func_name = "JSON_VALUE";
            default_format = JS_FORMAT_DEFAULT;
            break;
        case JSON_TABLE_OP:
            func_name = "JSON_TABLE";
            default_format = JS_FORMAT_JSONB;
            break;
        default:
            elog(ERROR, "invalid JsonFuncExpr op %d", (int) func->op);
            break;
    }

    // Validate FORMAT JSON usage (only meaningful for JSON_QUERY)
    if (func->output && func->op != JSON_QUERY_OP)
    {
        JsonFormat *format = func->output->returning->format;
        if (format->format_type != JS_FORMAT_DEFAULT || format->encoding != JS_ENC_DEFAULT)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("cannot specify FORMAT JSON in RETURNING clause of %s()",
                                  func_name),
                           parser_errposition(pstate, format->location)));
    }

    // Validate behavior clauses for each function type (extensive validation logic)
    // ... [Behavior validation for JSON_QUERY, JSON_EXISTS, JSON_VALUE] ...

    // Create the JsonExpr node
    jsexpr = makeNode(JsonExpr);
    jsexpr->location = func->location;
    jsexpr->op = func->op;
    jsexpr->column_name = func->column_name;

    // Transform context item to JSONB for jsonpath processing
    jsexpr->formatted_expr = transformJsonValueExpr(pstate, func_name,
                                                   func->context_item,
                                                   default_format,
                                                   JSONBOID, false);
    jsexpr->format = func->context_item->format;

    // Transform and validate path specification
    path_spec = transformExprRecurse(pstate, func->pathspec);
    path_spec = coerce_to_target_type(pstate, path_spec, exprType(path_spec),
                                     JSONPATHOID, -1,
                                     COERCION_EXPLICIT, COERCE_IMPLICIT_CAST,
                                     exprLocation(path_spec));
    if (path_spec == NULL)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("JSON path expression must be of type %s, not of type %s",
                              "jsonpath", format_type_be(exprType(path_spec))),
                       parser_errposition(pstate, exprLocation(path_spec))));
    jsexpr->path_spec = path_spec;

    // Transform PASSING arguments
    transformJsonPassingArgs(pstate, func_name, JS_FORMAT_JSONB,
                            func->passing, &jsexpr->passing_values,
                            &jsexpr->passing_names);

    // Transform output specification
    jsexpr->returning = transformJsonOutput(pstate, func->output, false);

    // Configure function-specific behavior and defaults
    switch (func->op)
    {
        case JSON_EXISTS_OP:
            // Returns boolean by default, handle non-boolean types for JSON_TABLE
            if (!OidIsValid(jsexpr->returning->typid))
            {
                jsexpr->returning->typid = BOOLOID;
                jsexpr->returning->typmod = -1;
            }
            if (jsexpr->returning->typid != BOOLOID)
                jsexpr->use_json_coercion = true;
            jsexpr->on_error = transformJsonBehavior(pstate, func->on_error,
                                                    JSON_BEHAVIOR_FALSE,
                                                    jsexpr->returning);
            break;

        case JSON_QUERY_OP:
            // Returns JSONB by default, handle quotes and wrapper behavior
            if (!OidIsValid(jsexpr->returning->typid))
            {
                jsexpr->returning->typid = JSONBOID;
                jsexpr->returning->typmod = -1;
            }
            jsexpr->omit_quotes = (func->quotes == JS_QUOTES_OMIT);
            jsexpr->wrapper = func->wrapper;
            if (jsexpr->returning->typid != JSONBOID || jsexpr->omit_quotes)
                jsexpr->use_json_coercion = true;
            jsexpr->on_empty = transformJsonBehavior(pstate, func->on_empty,
                                                    JSON_BEHAVIOR_NULL,
                                                    jsexpr->returning);
            jsexpr->on_error = transformJsonBehavior(pstate, func->on_error,
                                                    JSON_BEHAVIOR_NULL,
                                                    jsexpr->returning);
            break;

        case JSON_VALUE_OP:
            // Returns text by default, always omit quotes
            if (!OidIsValid(jsexpr->returning->typid))
            {
                jsexpr->returning->typid = TEXTOID;
                jsexpr->returning->typmod = -1;
            }
            jsexpr->returning->format->format_type = JS_FORMAT_DEFAULT;
            jsexpr->returning->format->encoding = JS_ENC_DEFAULT;
            jsexpr->omit_quotes = true;
            // Set up coercion for non-text types
            if (jsexpr->returning->typid != TEXTOID)
            {
                if (get_typtype(jsexpr->returning->typid) == TYPTYPE_DOMAIN &&
                    DomainHasConstraints(jsexpr->returning->typid))
                    jsexpr->use_json_coercion = true;
                else
                    jsexpr->use_io_coercion = true;
            }
            jsexpr->on_empty = transformJsonBehavior(pstate, func->on_empty,
                                                    JSON_BEHAVIOR_NULL,
                                                    jsexpr->returning);
            jsexpr->on_error = transformJsonBehavior(pstate, func->on_error,
                                                    JSON_BEHAVIOR_NULL,
                                                    jsexpr->returning);
            break;

        case JSON_TABLE_OP:
            if (!OidIsValid(jsexpr->returning->typid))
            {
                jsexpr->returning->typid = exprType(jsexpr->formatted_expr);
                jsexpr->returning->typmod = -1;
            }
            jsexpr->on_error = transformJsonBehavior(pstate, func->on_error,
                                                    JSON_BEHAVIOR_EMPTY_ARRAY,
                                                    jsexpr->returning);
            break;

        default:
            elog(ERROR, "invalid JsonFuncExpr op %d", (int) func->op);
            break;
    }

    return (Node *) jsexpr;
}
```