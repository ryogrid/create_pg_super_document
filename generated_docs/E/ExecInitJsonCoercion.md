# ExecInitJsonCoercion

## Location
[src/backend/executor/execExpr.c:4538-4560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L4538-L4560)

## Overview
Initializes a EEOP_JSONEXPR_COERCION step to coerce the value given in resv to the specified RETURNING type for JSON expressions.

## Definition

```c
static void
ExecInitJsonCoercion(ExprState *state, JsonReturning *returning,
					 ErrorSaveContext *escontext, bool omit_quotes,
					 bool exists_coerce,
					 Datum *resv, bool *resnull)
```
## Detailed Description
ExecInitJsonCoercion sets up a specialized coercion step for PostgreSQL's JSON expressions that converts JSON values to the target data types specified in the RETURNING clause. This function creates a single EEOP_JSONEXPR_COERCION evaluation step with all necessary configuration for the json_populate_type() function.

The function handles several specialized coercion scenarios:
- **Quote handling**: Can omit quotes from JSON string values when coercing to non-string types
- **EXISTS operation optimization**: Special handling for JSON_EXISTS operations with integer and domain type optimizations
- **Error context management**: Supports soft error handling through ErrorSaveContext
- **Domain constraints**: Recognizes when domain constraint checking is required
- **Type-specific optimizations**: Special casting logic for boolean results in EXISTS operations

The coercion step utilizes a cache (json_coercion_cache) that gets populated during execution for performance optimization.

## Parameters / Member Variables
- `*state`: ExprState structure being built for expression evaluation
- `*returning`: JsonReturning structure specifying the target type and type modifier
- `*escontext`: ErrorSaveContext pointer for soft error handling, can be NULL for hard errors
- `omit_quotes`: Boolean indicating whether to remove quotes from JSON string values during coercion
- `exists_coerce`: Boolean indicating whether this is for a JSON_EXISTS operation requiring special handling
- `*resv`: Datum pointer where the coerced result value should be stored
- `*resnull`: Boolean pointer where the result null flag should be stored
## Dependencies
- Functions called/Symbols referenced:
  - [getBaseType](../g/getBaseType.md)
  - [DomainHasConstraints](../D/DomainHasConstraints.md)
  - [ExprEvalPushStep](ExprEvalPushStep.md)
  - EEOP_JSONEXPR_COERCION
  - [JsonReturning](../J/JsonReturning.md)
  - [ErrorSaveContext](ErrorSaveContext.md)
  - [ExprEvalStep](ExprEvalStep.md)
  - INT4OID
- Called from (representative examples):
  - [ExecInitJsonExpr](ExecInitJsonExpr.md) (three different call sites for main result, ON ERROR, and ON EMPTY coercions)

## Notes and Other Information
- Located in src/backend/executor/execExpr.c (lines 4538-4560)
- This is a static helper function used exclusively by ExecInitJsonExpr
- The json_coercion_cache field is initialized as NULL and gets populated during execution for performance
- Implements special optimizations for JSON_EXISTS operations, including direct casting to INT4 for boolean results
- Supports domain constraint checking when the target type is a domain type
- The exists_cast_to_int optimization handles the common case where JSON_EXISTS results need to be converted to integers
- Essential for PostgreSQL's JSON/SQL standard compliance, enabling proper type coercion for JSON_VALUE and JSON_QUERY operations
- Works in conjunction with the json_populate_type() function during actual execution

## Simplified Source

```c
static void
ExecInitJsonCoercion(ExprState *state, JsonReturning *returning,
                     ErrorSaveContext *escontext, bool omit_quotes,
                     bool exists_coerce, Datum *resv, bool *resnull)
{
    ExprEvalStep scratch = {0};

    // Set up JSON coercion step for json_populate_type()
    scratch.opcode = EEOP_JSONEXPR_COERCION;
    scratch.resvalue = resv;
    scratch.resnull = resnull;

    // Configure target type information
    scratch.d.jsonexpr_coercion.targettype = returning->typid;
    scratch.d.jsonexpr_coercion.targettypmod = returning->typmod;
    scratch.d.jsonexpr_coercion.json_coercion_cache = NULL;
    scratch.d.jsonexpr_coercion.escontext = escontext;
    scratch.d.jsonexpr_coercion.omit_quotes = omit_quotes;
    scratch.d.jsonexpr_coercion.exists_coerce = exists_coerce;

    // Optimize for EXISTS operations
    scratch.d.jsonexpr_coercion.exists_cast_to_int = exists_coerce &&
        getBaseType(returning->typid) == INT4OID;
    scratch.d.jsonexpr_coercion.exists_check_domain = exists_coerce &&
        DomainHasConstraints(returning->typid);

    // Add the step to expression state
    ExprEvalPushStep(state, &scratch);
}
```