# ExecRunCompiledExpr

## Location
[src/backend/jit/llvm/llvmjit_expr.c:2684-2703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_expr.c#L2684-L2703)

## Overview
Executes a JIT-compiled expression for the first time, performing validation and optimization before redirecting future calls directly to the compiled function.

## Definition

```c
struct_gep(b,
									StructFunctionCallInfoData,
									v_fcinfo,
									FIELDNO_FUNCTIONCALLINFODATA_ISNULL,
									"v_fcinfo_isnull");
```
## Detailed Description
This function serves as a one-time initialization wrapper for JIT-compiled expressions. It is called only on the first execution of a compiled expression to perform several critical setup tasks:

1. **Expression Validation**: Calls CheckExprStillValid to ensure the compiled expression is still compatible with the current execution context. This check is necessary because certain changes (like schema modifications) could invalidate previously compiled code.

2. **Function Retrieval**: Uses llvm_get_function to obtain a function pointer to the compiled LLVM code. This call may trigger LLVM's optimization passes and code generation if they haven't been performed yet, which is why it's wrapped in fatal-on-OOM protection.

3. **Indirection Removal**: Updates the ExprState's evalfunc pointer to point directly to the compiled function, eliminating the overhead of this wrapper function for all future calls.

4. **Initial Execution**: Calls the compiled function with the provided parameters and returns its result.

The function implements a lazy evaluation pattern where the expensive operations (optimization, compilation, function pointer resolution) are deferred until the first actual execution, but then cached for optimal performance on subsequent calls.

## Parameters / Member Variables
- : The ExprState containing the compiled expression and its associated CompiledExprState private data
- : The expression evaluation context providing access to tuple slots and other execution state
- : Output parameter set to indicate whether the expression result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - CompiledExprState
  - CheckExprStillValid
  - llvm_get_function
- Called from (representative examples):
  - llvm_compile_expr

## Notes and Other Information
- This function is static and only used internally within the LLVM JIT expression compilation system
- Returns a Datum representing the expression result
- The function removes itself from the call path after the first execution for optimal performance
- Uses llvm_enter_fatal_on_oom/llvm_leave_fatal_on_oom to handle potential out-of-memory conditions during LLVM operations
- The Assert(func) ensures that function compilation succeeded before attempting to execute it
- This pattern ensures that the overhead of function resolution and validation only occurs once per compiled expression