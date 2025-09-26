# JsonExprState

## Location
[src/include/nodes/execnodes.h:1022-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1022-L1085)

## Overview
JsonExprState manages the execution state for JSON path expressions, handling JSON path evaluation, type coercion, error handling, and empty result behaviors during query execution.

## Definition

```c
typedef struct JsonExprState
{
	/* original expression node */
	JsonExpr   *jsexpr;

	/* value/isnull for formatted_expr */
	NullableDatum formatted_expr;

	/* value/isnull for pathspec */
	NullableDatum pathspec;

	/* JsonPathVariable entries for passing_values */
	List	   *args;

	/*
	 * Output variables that drive the EEOP_JUMP_IF_NOT_TRUE steps that are
	 * added for ON ERROR and ON EMPTY expressions, if any.
	 *
	 * Reset for each evaluation of EEOP_JSONEXPR_PATH.
	 */

	/* Set to true if jsonpath evaluation cause an error.  */
	NullableDatum error;

	/* Set to true if the jsonpath evaluation returned 0 items. */
	NullableDatum empty;

	/*
	 * Addresses of steps that implement the non-ERROR variant of ON EMPTY and
	 * ON ERROR behaviors, respectively.
	 */
	int			jump_empty;
	int			jump_error;

	/*
	 * Address of the step to coerce the result value of jsonpath evaluation
	 * to the RETURNING type.  -1 if no coercion if JsonExpr.use_io_coercion
	 * is true.
	 */
	int			jump_eval_coercion;

	/*
	 * Address to jump to when skipping all the steps after performing
	 * ExecEvalJsonExprPath() so as to return whatever the JsonPath* function
	 * returned as is, that is, in the cases where there's no error and no
	 * coercion is necessary.
	 */
	int			jump_end;

	/*
	 * RETURNING type input function invocation info when
	 * JsonExpr.use_io_coercion is true.
	 */
	FunctionCallInfo input_fcinfo;

	/*
	 * For error-safe evaluation of coercions.  When the ON ERROR behavior is
	 * not ERROR, a pointer to this is passed to ExecInitExprRec() when
	 * initializing the coercion expressions or to ExecInitJsonCoercion().
	 *
	 * Reset for each evaluation of EEOP_JSONEXPR_PATH.
	 */
	ErrorSaveContext escontext;
} JsonExprState;
```
## Detailed Description
JsonExprState contains the execution state for JSON path expressions, which are used to extract and manipulate data from JSON documents using JSONPath queries. This structure is too large to be inlined within other execution structures and manages complex evaluation scenarios including error handling (ON ERROR clauses), empty result handling (ON EMPTY clauses), type coercion to RETURNING types, and efficient step-based execution control. The structure supports PostgreSQL's SQL/JSON functionality, enabling advanced JSON querying capabilities within SQL statements.

## Parameters / Member Variables
- `*jsexpr`: Pointer to the original JsonExpr node containing the JSON path expression definition
- `formatted_expr`: NullableDatum holding the formatted JSON expression input and its null status
- `pathspec`: NullableDatum containing the JSONPath specification string and its null status
- `*args`: List of JsonPathVariable entries representing variables passed to the JSON path evaluation
- `error`: Output flag set to true if JSONPath evaluation encounters an error (reset per evaluation)
- `empty`: Output flag set to true if JSONPath evaluation returns zero items (reset per evaluation)
- `jump_empty`: Step address for implementing non-ERROR ON EMPTY behavior
- `jump_error`: Step address for implementing non-ERROR ON ERROR behavior
- `jump_eval_coercion`: Step address for result value coercion to RETURNING type (-1 if no coercion needed)
- `jump_end`: Step address to jump to when skipping post-evaluation steps for direct result return
- `input_fcinfo`: Function call info for RETURNING type input function when using I/O coercion
- `escontext`: Error context for safe evaluation of coercions when ON ERROR behavior is not ERROR
## Dependencies
- Functions called/Symbols referenced:
  - [JsonExpr](JsonExpr.md)
  - [NullableDatum](../N/NullableDatum.md)
  - [List](../L/List.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [ErrorSaveContext](../E/ErrorSaveContext.md)
- Called from (representative examples):
  - [ExecInitJsonExpr](../E/ExecInitJsonExpr.md)
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)
  - [ExecEvalJsonCoercionFinish](../E/ExecEvalJsonCoercionFinish.md)
  - [llvm_compile_expr](../l/llvm_compile_expr.md)

## Notes and Other Information
JsonExprState implements sophisticated control flow management through jump addresses, enabling efficient execution of complex JSON path expressions with multiple conditional branches. The structure is designed to handle SQL/JSON standard compliance, particularly the ON ERROR and ON EMPTY clauses that define behavior for exceptional conditions. The error and empty flags are reset for each evaluation, allowing reuse of the state structure across multiple evaluations. The integration with LLVM JIT compilation demonstrates its importance in performance-critical JSON processing operations.