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