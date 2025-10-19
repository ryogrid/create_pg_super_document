# l_call

## Location
[src/include/jit/llvmjit_emit.h:138-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L138-L150)

## Overview
A static inline function that provides a version-agnostic wrapper for LLVM's function call instruction, ensuring compatibility across different LLVM versions.

## Definition
```c
static inline LLVMValueRef l_call(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef fn, LLVMValueRef *args, int32 nargs, const char *name)
```

## Detailed Description
The `l_call` function is a compatibility wrapper for LLVM's function call instruction builder. It automatically selects the appropriate LLVM API function based on the LLVM version being used. For LLVM versions prior to 16, it uses `LLVMBuildCall`, while for version 16 and later, it uses `LLVMBuildCall2`. This abstraction allows PostgreSQL's JIT compilation code to work seamlessly across different LLVM versions without requiring conditional compilation throughout the codebase.

Function calls are essential in compiled code for invoking both user-defined functions and runtime library functions. This wrapper ensures that PostgreSQL's JIT compiler can generate function calls regardless of the underlying LLVM version.

## Parameters / Member Variables
- `b`: LLVM builder reference used to insert the instruction
- `t`: LLVM type reference specifying the function type (used in LLVM 16+)
- `fn`: LLVM value reference representing the function to call
- `args`: Array of LLVM value references representing the function arguments
- `nargs`: Number of arguments in the args array
- `name`: String name for the resulting LLVM value (for debugging/readability)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMBuildCall (LLVM < 16)
  - LLVMBuildCall2 (LLVM >= 16)
  - fn (parameter reference)
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (in llvmjit_deform.c)
  - [llvm_compile_expr](llvm_compile_expr.md) (extensively in llvmjit_expr.c)
  - [BuildV1Call](../B/BuildV1Call.md)
  - [build_EvalXFuncInt](../b/build_EvalXFuncInt.md)

## Notes and Other Information
- This function is part of PostgreSQL's JIT compilation infrastructure
- Critical for generating function calls in JIT-compiled code, including calls to PostgreSQL's runtime functions
- Used extensively throughout expression evaluation and tuple slot operations
- The conditional compilation ensures compatibility across LLVM version boundaries where call instruction APIs changed
- Essential for implementing complex PostgreSQL operations that require calling back into the PostgreSQL runtime from JIT-compiled code
- Enables the JIT compiler to generate calls to functions like memory allocation, error handling, and type-specific operations

## Simplified Source

```c
static inline LLVMValueRef
l_call(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef fn, LLVMValueRef *args, int32 nargs, const char *name)
{
    // Version-compatible function call instruction
    // LLVM 16+ requires explicit function type parameter
#if LLVM_VERSION_MAJOR < 16
    return LLVMBuildCall(b, fn, args, nargs, name);
#else
    return LLVMBuildCall2(b, t, fn, args, nargs, name);
#endif
}
```

This wrapper provides version-compatible function call operations. It's essential for generating calls to PostgreSQL runtime functions from JIT-compiled code, handling LLVM API differences transparently.