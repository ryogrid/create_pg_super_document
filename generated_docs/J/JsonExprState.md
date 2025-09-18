# JsonExprState

## Location
[src/include/nodes/execnodes.h:1022-1085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1022-L1085)

## Overview
JsonExprState manages the execution state for JSON path expressions, handling JSON path evaluation, type coercion, error handling, and empty result behaviors during query execution.

## Definition


## Detailed Description
JsonExprState contains the execution state for JSON path expressions, which are used to extract and manipulate data from JSON documents using JSONPath queries. This structure is too large to be inlined within other execution structures and manages complex evaluation scenarios including error handling (ON ERROR clauses), empty result handling (ON EMPTY clauses), type coercion to RETURNING types, and efficient step-based execution control. The structure supports PostgreSQL's SQL/JSON functionality, enabling advanced JSON querying capabilities within SQL statements.

## Parameters / Member Variables
- : Pointer to the original JsonExpr node containing the JSON path expression definition
- : NullableDatum holding the formatted JSON expression input and its null status
- : NullableDatum containing the JSONPath specification string and its null status
- : List of JsonPathVariable entries representing variables passed to the JSON path evaluation
- : Output flag set to true if JSONPath evaluation encounters an error (reset per evaluation)
- : Output flag set to true if JSONPath evaluation returns zero items (reset per evaluation)  
- : Step address for implementing non-ERROR ON EMPTY behavior
- : Step address for implementing non-ERROR ON ERROR behavior
- : Step address for result value coercion to RETURNING type (-1 if no coercion needed)
- : Step address to jump to when skipping post-evaluation steps for direct result return
- : Function call info for RETURNING type input function when using I/O coercion
- : Error context for safe evaluation of coercions when ON ERROR behavior is not ERROR

## Dependencies
- Functions called/Symbols referenced:
  - JsonExpr
  - [NullableDatum](../N/NullableDatum.md)
  - [List](../L/List.md)
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [ErrorSaveContext](../E/ErrorSaveContext.md)
- Called from (representative examples):
  - [ExecInitJsonExpr](../E/ExecInitJsonExpr.md)
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)
  - [ExecEvalJsonCoercionFinish](../E/ExecEvalJsonCoercionFinish.md)
  - llvm_compile_expr

## Notes and Other Information
JsonExprState implements sophisticated control flow management through jump addresses, enabling efficient execution of complex JSON path expressions with multiple conditional branches. The structure is designed to handle SQL/JSON standard compliance, particularly the ON ERROR and ON EMPTY clauses that define behavior for exceptional conditions. The error and empty flags are reset for each evaluation, allowing reuse of the state structure across multiple evaluations. The integration with LLVM JIT compilation demonstrates its importance in performance-critical JSON processing operations.