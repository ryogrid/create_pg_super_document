# _PG_jit_provider_init

## Location
[src/backend/jit/llvm/llvmjit.c:164-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L164-L185)

## Overview
The initialization function for PostgreSQL's LLVM JIT provider that sets up callback functions for the JIT compilation system.

## Definition
void _PG_jit_provider_init(JitProviderCallbacks *cb)

## Detailed Description
_PG_jit_provider_init is the entry point function called by PostgreSQL's JIT infrastructure to initialize the LLVM JIT provider. This function is automatically discovered and called by PostgreSQL when loading the LLVM JIT shared library. It populates the JitProviderCallbacks structure with function pointers to LLVM-specific implementations of the JIT operations.

The function sets up three key callback functions:
- reset_after_error: for cleaning up JIT state after errors
- release_context: for properly releasing JIT compilation contexts  
- compile_expr: for compiling SQL expressions to native code

This follows PostgreSQL's plugin architecture pattern where providers implement a standard interface through callback functions.

## Parameters / Member Variables
- cb: Pointer to JitProviderCallbacks structure that will be populated with LLVM-specific function pointers

## Dependencies
- Functions called/Symbols referenced:
  - llvm_reset_after_error (assigned to reset_after_error callback)
  - [llvm_release_context](../l/llvm_release_context.md) (assigned to release_context callback)
  - llvm_compile_expr (assigned to compile_expr callback)
- Called from (representative examples):
  - PostgreSQL JIT infrastructure during provider loading

## Notes and Other Information
- This function name follows PostgreSQL's convention for provider initialization functions
- The underscore prefix indicates this is a special function recognized by PostgreSQL's dynamic loading system
- Part of the JIT provider plugin interface defined in src/include/jit/jit.h
- Must be present and properly named for the LLVM JIT provider to be recognized by PostgreSQL
- Located in src/backend/jit/llvm/llvmjit.c:164-185