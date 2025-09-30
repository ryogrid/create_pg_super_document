# ExecInitJsonExpr

## Location
[src/backend/executor/execExpr.c:4236-4537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L4236-L4537)

## Overview
Initializes expression evaluation steps for a JsonExpr and its various subsidiary expressions, including formatted_expr, path_spec, passing arguments, coercion handling, and ON ERROR/ON EMPTY behaviors.

## Definition

```c
static void
ExecInitJsonExpr(JsonExpr *jsexpr, ExprState *state,
				 Datum *resv, bool *resnull,
				 ExprEvalStep *scratch)
```
## Detailed Description
ExecInitJsonExpr is a comprehensive function that sets up the complete evaluation infrastructure for PostgreSQL's JSON expressions. It orchestrates the initialization of multiple components:

**Core Expression Evaluation:**
- Initializes the formatted_expr (input JSON data) and path_spec (JSONPath query) evaluation
- Sets up PASSING arguments as JsonPathVariable structures for use in JSONPath queries
- Adds null-checking jumps to handle NULL input gracefully

**JSONPath Execution:**
- Creates the main EEOP_JSONEXPR_PATH step that performs the actual JSONPath evaluation
- Manages the JsonExprState structure containing all necessary runtime information

**Coercion Handling:**
- Supports both JSON coercion (ExecInitJsonCoercion) and I/O coercion via type input functions
- Sets up function call information for I/O coercion with pre-loaded constant arguments
- Implements soft error handling through ErrorSaveContext when ON ERROR is not ERROR

**Behavior Handlers:**
- Implements ON ERROR behavior with conditional expression evaluation and coercion
- Implements ON EMPTY behavior with similar conditional evaluation
- Optimizes common NULL-valued expressions unless returning a domain type
- Uses jump instructions for efficient control flow

The function carefully manages jump targets and implements complex control flow to handle all the conditional behaviors efficiently.

## Parameters / Member Variables
- : JsonExpr structure containing the JSON expression definition and behavior specifications
- : ExprState structure being built for expression evaluation
- : Datum pointer where the result value should be stored
- : Boolean pointer where the result null flag should be stored
- : ExprEvalStep structure used as a template for building evaluation steps

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [get_typtype](../g/get_typtype.md)
  - [ExecInitExprRec](ExecInitExprRec.md)
  - [lappend_int](../l/lappend_int.md)
  - [ExprEvalPushStep](ExprEvalPushStep.md)
  - [lappend](../l/lappend.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info](../f/fmgr_info.md)
  - fmgr_info_set_expr
  - InitFunctionCallInfoData
  - SizeForFunctionCallInfo
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - [ExecInitJsonCoercion](ExecInitJsonCoercion.md)
  - lfirst_int
  - [JsonExprState](../J/JsonExprState.md)
  - [JsonPathVariable](../J/JsonPathVariable.md)
  - [ErrorSaveContext](ErrorSaveContext.md)
  - EEOP_JUMP_IF_NULL
  - EEOP_JSONEXPR_PATH
  - EEOP_CONST
  - EEOP_JSONEXPR_COERCION_FINISH
  - EEOP_JUMP_IF_NOT_TRUE
  - EEOP_JUMP
  - JSON_BEHAVIOR_ERROR
  - JSON_EXISTS_OP
  - TYPTYPE_DOMAIN
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md)

## Notes and Other Information
- Located in src/backend/executor/execExpr.c (lines 4236-4537)
- This is a static function that handles the complex initialization of JSON expressions
- Implements sophisticated optimization by avoiding unnecessary steps for NULL-valued ON ERROR/ON EMPTY expressions unless returning domain types
- Supports both JSON coercion and I/O coercion paths depending on the expression configuration
- Uses ErrorSaveContext for soft error handling, allowing errors to be caught and handled according to ON ERROR behavior
- Manages complex control flow with multiple jump targets for efficient evaluation
- Pre-loads constant arguments (typioparam and typmod) for I/O coercion function calls
- Essential for PostgreSQL's JSON/SQL standard compliance, supporting JSON_VALUE, JSON_QUERY, and JSON_EXISTS operations

## Simplified Source

```c
static void
ExecInitJsonExpr(JsonExpr *jsexpr, ExprState *state,
                 Datum *resv, bool *resnull,
                 ExprEvalStep *scratch)
{
    JsonExprState *jsestate = palloc0(sizeof(JsonExprState));
    List *jumps_return_null = NIL;
    List *jumps_to_end = NIL;
    bool returning_domain = get_typtype(jsexpr->returning->typid) == TYPTYPE_DOMAIN;

    jsestate->jsexpr = jsexpr;

    // Initialize formatted_expr evaluation
    ExecInitExprRec((Expr *) jsexpr->formatted_expr, state,
                    &jsestate->formatted_expr.value,
                    &jsestate->formatted_expr.isnull);

    // Add jump to return NULL if formatted_expr is NULL
    jumps_return_null = lappend_int(jumps_return_null, state->steps_len);
    scratch->opcode = EEOP_JUMP_IF_NULL;
    scratch->resnull = &jsestate->formatted_expr.isnull;
    ExprEvalPushStep(state, scratch);

    // Initialize pathspec evaluation
    ExecInitExprRec((Expr *) jsexpr->path_spec, state,
                    &jsestate->pathspec.value,
                    &jsestate->pathspec.isnull);

    // Add jump to return NULL if pathspec is NULL
    jumps_return_null = lappend_int(jumps_return_null, state->steps_len);
    scratch->opcode = EEOP_JUMP_IF_NULL;
    scratch->resnull = &jsestate->pathspec.isnull;
    ExprEvalPushStep(state, scratch);

    // Initialize PASSING arguments
    jsestate->args = NIL;
    foreach(argexprlc, jsexpr->passing_values)
    {
        Expr *argexpr = (Expr *) lfirst(argexprlc);
        String *argname = lfirst_node(String, argnamelc);
        JsonPathVariable *var = palloc(sizeof(*var));

        var->name = argname->sval;
        var->namelen = strlen(var->name);
        var->typid = exprType((Node *) argexpr);
        var->typmod = exprTypmod((Node *) argexpr);

        ExecInitExprRec((Expr *) argexpr, state, &var->value, &var->isnull);
        jsestate->args = lappend(jsestate->args, var);
    }

    // Main JSONPath evaluation step
    scratch->opcode = EEOP_JSONEXPR_PATH;
    scratch->resvalue = resv;
    scratch->resnull = resnull;
    scratch->d.jsonexpr.jsestate = jsestate;
    ExprEvalPushStep(state, scratch);

    // Set up NULL return step for NULL inputs
    foreach(lc, jumps_return_null)
    {
        ExprEvalStep *as = &state->steps[lfirst_int(lc)];
        as->d.jump.jumpdone = state->steps_len;
    }
    scratch->opcode = EEOP_CONST;
    scratch->d.constval.isnull = true;
    ExprEvalPushStep(state, scratch);

    // Initialize coercion handling
    if (jsexpr->use_json_coercion)
    {
        jsestate->jump_eval_coercion = state->steps_len;
        ExecInitJsonCoercion(state, jsexpr->returning, escontext,
                            jsexpr->omit_quotes,
                            jsexpr->op == JSON_EXISTS_OP,
                            resv, resnull);
    }
    else if (jsexpr->use_io_coercion)
    {
        // Setup I/O coercion function call info
        getTypeInputInfo(jsexpr->returning->typid, &typinput, &typioparam);
        // Initialize function call with constant arguments
        jsestate->input_fcinfo = setup_fcinfo_for_input_function();
    }

    // Initialize ON ERROR behavior
    if (jsexpr->on_error->btype != JSON_BEHAVIOR_ERROR && needs_error_handling)
    {
        jsestate->jump_error = state->steps_len;
        setup_error_handling_steps();
    }

    // Initialize ON EMPTY behavior
    if (jsexpr->on_empty != NULL && needs_empty_handling)
    {
        jsestate->jump_empty = state->steps_len;
        setup_empty_handling_steps();
    }

    // Finalize all jump targets
    foreach(lc, jumps_to_end)
    {
        ExprEvalStep *as = &state->steps[lfirst_int(lc)];
        as->d.jump.jumpdone = state->steps_len;
    }

    jsestate->jump_end = state->steps_len;
}
```