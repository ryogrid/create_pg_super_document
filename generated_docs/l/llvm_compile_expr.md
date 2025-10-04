# llvm_compile_expr

## Location
[src/backend/jit/llvm/llvmjit_expr.c:78-2683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_expr.c#L78-L2683)

## Overview
Compiles an ExprState into optimized LLVM IR code for high-performance expression evaluation, serving as the main entry point for PostgreSQL's JIT expression compilation.

## Definition

```c
struct_gep(b,
							   StructExprState,
							   v_state,
							   FIELDNO_EXPRSTATE_RESVALUE,
							   "v.state.resvalue");
```
## Detailed Description
This function is the core of PostgreSQL's LLVM-based Just-In-Time (JIT) compilation system for expressions. It takes an ExprState containing a series of expression evaluation steps and compiles them into optimized LLVM IR code that can be executed much faster than the interpreted version.

The compilation process involves several key phases:

1. **Context Setup**: Creates or retrieves an existing LLVM JIT context from the parent plan state
2. **Function Generation**: Creates a new LLVM function with the signature matching ExecInterpExprStillValid
3. **Variable Initialization**: Sets up LLVM variables corresponding to expression state components (slots, values, nulls arrays, etc.)
4. **Step Compilation**: Iterates through each ExprEvalStep and generates corresponding LLVM IR code for operations like:
   - Variable access (INNER_VAR, OUTER_VAR, SCAN_VAR)
   - Function calls (FUNCEXPR, FUNCEXPR_STRICT)
   - Boolean expressions (BOOL_AND, BOOL_OR, BOOL_NOT)
   - Null tests and type coercions
   - Aggregate operations
   - And many other expression evaluation operations

5. **Optimization**: The generated LLVM IR is subject to LLVM's optimization passes
6. **Function Registration**: The compiled function is stored in a CompiledExprState for later execution

The function handles complex control flow through basic blocks and conditional branches, implementing the same semantics as the interpreted expression evaluator but with significantly better performance for frequently executed expressions.

## Parameters / Member Variables
- : The ExprState containing the expression evaluation steps to be compiled. Must have a valid parent PlanState with an associated EState for JIT context management.

## Dependencies
- Functions called/Symbols referenced:
  - [llvm_create_context](llvm_create_context.md)
  - [llvm_mutable_module](llvm_mutable_module.md)  
  - [llvm_expand_funcname](llvm_expand_funcname.md)
  - [llvm_pg_var_func_type](llvm_pg_var_func_type.md)
  - [llvm_copy_attributes](llvm_copy_attributes.md)
  - [AttributeTemplate](../A/AttributeTemplate.md)
  - [BuildV1Call](../B/BuildV1Call.md)
  - build_EvalXFunc
  - [slot_compile_deform](../s/slot_compile_deform.md)
  - [CompiledExprState](../C/CompiledExprState.md)
  - [ExecRunCompiledExpr](../E/ExecRunCompiledExpr.md)
- Called from (representative examples):
  - [_PG_jit_provider_init](../P/_PG_jit_provider_init.md)

## Notes and Other Information
- Returns true on successful compilation, false on failure
- Requires LLVM JIT to be enabled and available in the PostgreSQL build
- The compiled function has the same signature as ExecInterpExprStillValid for seamless integration
- Performance instrumentation tracks compilation time separately from deform compilation time
- Memory management uses palloc for temporary allocations during compilation
- The function sets up extensive basic block structure to handle complex expression control flow
- Supports compilation of tuple deforming operations when PGJIT_DEFORM flag is enabled

## Simplified Source

```c
bool llvm_compile_expr(ExprState *state) {
    LLVMJitContext *context;
    LLVMBuilderRef b;
    LLVMModuleRef mod;
    LLVMValueRef eval_fn;
    CompiledExprState *cstate;
    int step_count = state->steps_len;

    // Get or create JIT context from parent plan state
    context = llvm_jit_context(state->parent);
    if (!context)
        return false;

    // Create mutable module for this compilation
    mod = llvm_mutable_module(context);
    b = LLVMCreateBuilderInContext(LLVMGetModuleContext(mod));

    // Create function with ExecInterpExprStillValid signature
    eval_fn = llvm_create_function(mod, "evalexpr", llvm_pg_var_func_type("b"));
    LLVMSetLinkage(eval_fn, LLVMInternalLinkage);

    // Set up basic blocks for control flow
    LLVMBasicBlockRef entry_block = LLVMAppendBasicBlock(eval_fn, "entry");
    LLVMBasicBlockRef *op_blocks = palloc(sizeof(LLVMBasicBlockRef) * step_count);

    // Initialize LLVM variables for expression state components
    // (slots, values, nulls arrays, expression context, etc.)
    llvm_setup_expr_vars(b, eval_fn, state);

    // Compile each expression evaluation step
    for (int opno = 0; opno < step_count; opno++) {
        ExprEvalStep *op = &state->steps[opno];

        op_blocks[opno] = LLVMAppendBasicBlock(eval_fn, "step");
        LLVMPositionBuilderAtEnd(b, op_blocks[opno]);

        // Generate LLVM IR based on step operation type
        switch (op->opcode) {
            case EEOP_INNER_VAR:
            case EEOP_OUTER_VAR:
            case EEOP_SCAN_VAR:
                // Compile variable access
                llvm_compile_var_access(b, op);
                break;

            case EEOP_FUNCEXPR:
            case EEOP_FUNCEXPR_STRICT:
                // Compile function calls
                llvm_compile_func_expr(b, op);
                break;

            case EEOP_BOOL_AND:
            case EEOP_BOOL_OR:
            case EEOP_BOOL_NOT:
                // Compile boolean expressions
                llvm_compile_bool_expr(b, op);
                break;

            // ... handle other expression types ...

            default:
                // Fall back to interpreted evaluation for unsupported ops
                llvm_compile_fallback(b, op);
                break;
        }

        // Jump to next step or return
        if (opno + 1 < step_count)
            LLVMBuildBr(b, op_blocks[opno + 1]);
        else
            LLVMBuildRetVoid(b);
    }

    // Create compiled expression state
    cstate = palloc0(sizeof(CompiledExprState));
    cstate->context = context;
    cstate->evalfunc = (ExprStateEvalFunc) llvm_get_function(context, eval_fn);

    // Replace expression evaluator
    state->evalfunc_private = cstate;
    state->evalfunc = ExecRunCompiledExpr;

    LLVMDisposeBuilder(b);
    return true;
}
```