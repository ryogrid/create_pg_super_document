# llvm_mutable_module

## Location
[src/backend/jit/llvm/llvmjit.c:335-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L335-L359)

## Overview
Returns a mutable LLVM module from the JIT context, creating a new module if one doesn't already exist.

## Definition

```c
LLVMModuleRef
llvm_mutable_module(LLVMJitContext *context)
```
## Detailed Description
This function provides access to a mutable LLVM module that can be modified by adding new functions or other LLVM IR constructs. It implements lazy initialization - if the context doesn't have a module, it creates a new one with the PostgreSQL-specific target configuration (triple and data layout). When creating a new module, it resets the compiled flag and assigns a new generation number for tracking purposes.

## Parameters / Member Variables
- : LLVMJitContext pointer containing the LLVM compilation context and module state

## Dependencies
- Functions called/Symbols referenced:
  - llvm_assert_in_fatal_section (safety assertion)
  - LLVMModuleCreateWithNameInContext (LLVM API - creates module)
  - LLVMSetTarget (LLVM API - sets target triple)
  - LLVMSetDataLayout (LLVM API - sets data layout)
  - llvm_context (global LLVM context)
  - llvm_triple (global target triple)
  - llvm_layout (global data layout)
  - llvm_generation (global generation counter)
- Called from:
  - [slot_compile_deform](../s/slot_compile_deform.md) (tuple deforming compilation)
  - llvm_compile_expr (expression compilation)

## Notes and Other Information
- Requires being called within a fatal section (asserted by llvm_assert_in_fatal_section)
- Uses lazy initialization pattern - modules are created only when needed
- Each new module gets a unique generation number for tracking
- Module name is hardcoded as "pg" (PostgreSQL)
- Resets the compiled flag when creating a new module, indicating fresh compilation state