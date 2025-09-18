# l_ptr

## Location
[src/include/jit/llvmjit_emit.h:39-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L39-L47)

## Overview
A static inline utility function that creates an LLVM pointer type from a given base type for use in JIT compilation.

## Definition
```c
static inline LLVMTypeRef
l_ptr(LLVMTypeRef t)
```

## Detailed Description
This function is a simple wrapper around LLVM's `LLVMPointerType` function that creates pointer types for LLVM IR generation. It takes any LLVM type and returns the corresponding pointer type with address space 0 (the default address space). This is a fundamental building block in PostgreSQL's JIT compilation system for creating typed pointers that are used throughout the generated LLVM code.

The function abstracts the LLVM C API call and provides a more concise interface for creating pointer types, which are frequently needed when generating code that manipulates PostgreSQL's data structures and function calls.

## Parameters / Member Variables
- `t`: The base LLVM type for which to create a pointer type

## Dependencies
- Functions called/Symbols referenced:
  - LLVMPointerType (LLVM C API function)
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (in llvmjit_deform.c for tuple slot deformation)
  - llvm_compile_expr (extensively used throughout llvmjit_expr.c for expression compilation)
  - BuildV1Call (in llvmjit_expr.c for function call generation)
  - build_EvalXFuncInt (in llvmjit_expr.c for evaluation function building)
  - create_LifetimeEnd (in llvmjit_expr.c for lifetime management)
  - [l_mcxt_switch](l_mcxt_switch.md) (in llvmjit_emit.h for memory context switching)

## Notes and Other Information
- This is a header-only inline function defined in src/include/jit/llvmjit_emit.h
- Always uses address space 0, which is the default address space in most architectures
- Part of PostgreSQL's JIT compilation infrastructure for type-safe code generation
- Used extensively in both expression compilation and tuple deformation code
- Provides a cleaner interface compared to directly calling LLVMPointerType
- Essential for maintaining type safety when generating LLVM IR that manipulates PostgreSQL data structures