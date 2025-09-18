# ExecInitJsonExpr

## Location
[src/backend/executor/execExpr.c:4236-4537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L4236-L4537)

## Overview
Initializes expression evaluation steps for a JsonExpr and its various subsidiary expressions, including formatted_expr, path_spec, passing arguments, coercion handling, and ON ERROR/ON EMPTY behaviors.

## Definition


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
  - lappend_int
  - [ExprEvalPushStep](ExprEvalPushStep.md)
  - lappend
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
  - JsonPathVariable
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