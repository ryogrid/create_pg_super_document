# compute_function_attributes

## Location
[src/backend/commands/functioncmds.c:714-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L714-L850)

## Overview
Parses and validates function definition options from SQL CREATE FUNCTION/PROCEDURE statements, converting them into internal attribute structures.

## Definition
```c
static void compute_function_attributes(ParseState *pstate,
                                      bool is_procedure,
                                      List *options,
                                      List **as,
                                      char **language,
                                      Node **transform,
                                      bool *windowfunc_p,
                                      char *volatility_p,
                                      bool *strict_p,
                                      bool *security_definer,
                                      bool *leakproof_p,
                                      ArrayType **proconfig,
                                      float4 *procost,
                                      float4 *prorows,
                                      Oid *prosupport,
                                      char *parallel_p)
```

## Detailed Description
This static function dissects the list of options assembled in gram.y into individual function attributes. It processes various function/procedure definition options including AS clause (function body), language specification, transform functions, window function flag, volatility, strictness, security definer mode, leak-proof property, configuration parameters, cost estimates, row estimates, support functions, and parallel safety.

The function validates each option for conflicts (duplicate specifications) and appropriateness (e.g., window functions not allowed in procedures). It delegates common attribute processing to compute_common_attribute() and uses specialized interpretation functions for complex attributes like volatility, support functions, and parallel safety.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and location tracking
- `is_procedure`: Boolean flag indicating if this is a procedure (not function) definition
- `options`: List of DefElem options from the SQL statement
- `as`: Output pointer for AS clause (function body/source)
- `language`: Output pointer for language name string
- `transform`: Output pointer for transform function specification
- `windowfunc_p`: Output pointer for window function flag
- `volatility_p`: Output pointer for volatility classification
- `strict_p`: Output pointer for strict (returns null on null input) flag
- `security_definer`: Output pointer for security definer mode flag
- `leakproof_p`: Output pointer for leak-proof property flag
- `proconfig`: Output pointer for configuration parameter array
- `procost`: Output pointer for execution cost estimate
- `prorows`: Output pointer for rows returned estimate (for SRFs)
- `prosupport`: Output pointer for support function OID
- `parallel_p`: Output pointer for parallel safety classification

## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type for definition elements)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md) (reports conflicting option errors)
  - [compute_common_attribute](compute_common_attribute.md) (processes common function/procedure attributes)
  - [interpret_func_volatility](../i/interpret_func_volatility.md) (interprets volatility specifications)
  - [interpret_func_support](../i/interpret_func_support.md) (validates and resolves support functions)
  - [interpret_func_parallel](../i/interpret_func_parallel.md) (interprets parallel safety specifications)
  - [update_proconfig_value](../u/update_proconfig_value.md) (processes configuration parameter settings)
  - boolVal (extracts boolean values from nodes)
  - [defGetNumeric](../d/defGetNumeric.md) (extracts numeric values from DefElem)
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md) (src/backend/commands/functioncmds.c:1075)

## Notes and Other Information
- Validates that COST and ROWS values are positive when specified
- Prevents window function specification in procedure definitions
- Handles both function-specific and common attributes through delegation
- Provides comprehensive error reporting with parse location information
- Central function in PostgreSQL's function/procedure DDL processing pipeline
- Part of the function creation workflow that translates SQL syntax into catalog entries

## Simplified Source
```c
static void
compute_function_attributes(ParseState *pstate, bool is_procedure, List *options,
                           List **as, char **language, Node **transform,
                           bool *windowfunc_p, char *volatility_p, bool *strict_p,
                           bool *security_definer, bool *leakproof_p,
                           ArrayType **proconfig, float4 *procost, float4 *prorows,
                           Oid *prosupport, char *parallel_p)
{
    DefElem *as_item = NULL, *language_item = NULL, *transform_item = NULL;
    DefElem *windowfunc_item = NULL, *volatility_item = NULL, *strict_item = NULL;
    DefElem *security_item = NULL, *leakproof_item = NULL;
    DefElem *cost_item = NULL, *rows_item = NULL, *support_item = NULL, *parallel_item = NULL;
    List *set_items = NIL;

    // Process each option in the list
    foreach(option, options) {
        DefElem *defel = (DefElem *) lfirst(option);

        // Handle function-specific attributes
        if (strcmp(defel->defname, "as") == 0) {
            if (as_item) errorConflictingDefElem(defel, pstate);
            as_item = defel;
        }
        else if (strcmp(defel->defname, "language") == 0) {
            if (language_item) errorConflictingDefElem(defel, pstate);
            language_item = defel;
        }
        else if (strcmp(defel->defname, "transform") == 0) {
            if (transform_item) errorConflictingDefElem(defel, pstate);
            transform_item = defel;
        }
        else if (strcmp(defel->defname, "window") == 0) {
            if (windowfunc_item) errorConflictingDefElem(defel, pstate);
            if (is_procedure)
                ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                               errmsg("invalid attribute in procedure definition")));
            windowfunc_item = defel;
        }
        // Handle common attributes through helper function
        else if (compute_common_attribute(pstate, is_procedure, defel,
                                         &volatility_item, &strict_item, &security_item,
                                         &leakproof_item, &set_items, &cost_item,
                                         &rows_item, &support_item, &parallel_item)) {
            continue;
        }
        else
            elog(ERROR, "option \"%s\" not recognized", defel->defname);
    }

    // Extract values from collected items
    if (as_item) *as = (List *) as_item->arg;
    if (language_item) *language = strVal(language_item->arg);
    if (transform_item) *transform = transform_item->arg;
    if (windowfunc_item) *windowfunc_p = boolVal(windowfunc_item->arg);
    if (volatility_item) *volatility_p = interpret_func_volatility(volatility_item);
    if (strict_item) *strict_p = boolVal(strict_item->arg);
    if (security_item) *security_definer = boolVal(security_item->arg);
    if (leakproof_item) *leakproof_p = boolVal(leakproof_item->arg);
    if (set_items) *proconfig = update_proconfig_value(NULL, set_items);

    // Validate and extract cost/rows
    if (cost_item) {
        *procost = defGetNumeric(cost_item);
        if (*procost <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("COST must be positive")));
    }
    if (rows_item) {
        *prorows = defGetNumeric(rows_item);
        if (*prorows <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("ROWS must be positive")));
    }

    if (support_item) *prosupport = interpret_func_support(support_item);
    if (parallel_item) *parallel_p = interpret_func_parallel(parallel_item);
}
```