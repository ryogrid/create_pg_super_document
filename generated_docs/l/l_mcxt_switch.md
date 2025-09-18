# l_mcxt_switch

## Location
[src/include/jit/llvmjit_emit.h:251-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L251-L268)

## Overview
A utility function that generates LLVM code to switch PostgreSQL's current memory context and returns the previous context.

## Definition


## Detailed Description
This function generates LLVM IR code that implements PostgreSQL's memory context switching mechanism at the JIT level. Memory contexts are a fundamental part of PostgreSQL's memory management system, providing hierarchical memory allocation with automatic cleanup capabilities. The function emits code that:

1. Accesses the global `CurrentMemoryContext` variable
2. Loads the current memory context value to return as the previous context
3. Stores the new memory context as the current one

The function handles the case where the `CurrentMemoryContext` global variable hasn't been declared in the current LLVM module by adding it as a global variable reference. This ensures that JIT-compiled code can properly interact with PostgreSQL's memory management system, which is crucial for functions that allocate memory or need to ensure proper memory context for operations.

## Parameters / Member Variables
- `mod`: The LLVM module where the global variable will be declared if needed
- `b`: The LLVM builder used to emit the load and store instructions
- `nc`: The new memory context to switch to

## Dependencies
- Functions called/Symbols referenced:
  - `LLVMGetNamedGlobal`: To check if CurrentMemoryContext global already exists
  - `LLVMAddGlobal`: To declare the CurrentMemoryContext global if not present
  - [l_ptr](l_ptr.md): To create pointer types for the MemoryContextData structure
  - [l_load](l_load.md): To generate a load instruction for the current memory context
  - `LLVMBuildStore`: To generate a store instruction for the new memory context
- Called from (representative examples):
  - `llvm_compile_expr`: Used when compiling expressions that need to manage memory contexts during execution

## Notes and Other Information
- Returns the previous memory context, allowing for proper context restoration after operations
- Essential for maintaining PostgreSQL's memory management semantics in JIT-compiled code
- The `CurrentMemoryContext` is a global variable that tracks the active memory allocation context
- Memory context switching is performance-critical in PostgreSQL, making JIT optimization valuable
- Used in expression compilation where functions might allocate memory or require specific memory contexts
- The function ensures that JIT-compiled code properly integrates with PostgreSQL's error handling and memory cleanup mechanisms