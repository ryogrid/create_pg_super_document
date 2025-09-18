# llvm_expand_funcname

## Location
[src/backend/jit/llvm/llvmjit.c:360-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L360-L380)

## Overview
Generates unique, non-conflicting function names for LLVM IR functions by appending generation and counter suffixes to a base name.

## Definition


## Detailed Description
This utility function creates unique function names to avoid naming conflicts when adding multiple functions to an LLVM module. It combines the base function name with the module's generation number and an incrementing counter to ensure uniqueness. The naming scheme was specifically designed to be debugger-friendly, avoiding dots that some tools like GDB don't handle well.

The function also increments the instrumentation counter for tracking the number of functions created during JIT compilation.

## Parameters / Member Variables
- : LLVMJitContext pointer containing the module generation and counter state
- : Base name for the function that will be made unique

## Dependencies
- Functions called/Symbols referenced:
  - Assert (PostgreSQL assertion macro)
  - [psprintf](../p/psprintf.md) (PostgreSQL string formatting function)
  - context->module_generation (module generation number)
  - context->counter (function counter)
  - context->base.instr.created_functions (instrumentation counter)
- Called from:
  - [slot_compile_deform](../s/slot_compile_deform.md) (tuple deforming compilation)
  - llvm_compile_expr (expression compilation)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Naming format:  (e.g., "eval_expr_123_5")
- Previously used dots as separators but changed to underscores for better tool compatibility
- Requires that context->module is not NULL (asserted)
- Increments both the function counter and instrumentation tracking