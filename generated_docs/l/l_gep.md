# l_gep

## Location
[src/include/jit/llvmjit_emit.h:118-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L118-L127)

## Overview
A static inline function that provides a version-agnostic wrapper for LLVM's GetElementPtr (GEP) instruction, ensuring compatibility across different LLVM versions.

## Definition
```c
static inline LLVMValueRef l_gep(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, LLVMValueRef *indices, int32 nindices, const char *name)
```

## Detailed Description
The `l_gep` function is a compatibility wrapper for LLVM's GetElementPtr instruction builder. It automatically selects the appropriate LLVM API function based on the LLVM version being used. For LLVM versions prior to 16, it uses `LLVMBuildGEP`, while for version 16 and later, it uses `LLVMBuildGEP2`. This abstraction allows PostgreSQL's JIT compilation code to work seamlessly across different LLVM versions without requiring conditional compilation throughout the codebase.

The GetElementPtr instruction is fundamental in LLVM for computing addresses of sub-elements of aggregates (like arrays and structures). It performs pointer arithmetic in a type-safe manner.

## Parameters / Member Variables
- `b`: LLVM builder reference used to insert the instruction
- `t`: LLVM type reference specifying the base type for the GEP operation (used in LLVM 16+)
- `v`: LLVM value reference representing the base pointer
- `indices`: Array of LLVM value references representing the indices for the GEP operation
- `nindices`: Number of indices in the indices array
- `name`: String name for the resulting LLVM value (for debugging/readability)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMBuildGEP (LLVM < 16)
  - LLVMBuildGEP2 (LLVM >= 16)
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (multiple calls in llvmjit_deform.c)
  - [llvm_compile_expr](llvm_compile_expr.md) (multiple calls in llvmjit_expr.c)
  - [l_load_gep1](l_load_gep1.md)

## Notes and Other Information
- This function is part of PostgreSQL's JIT compilation infrastructure
- The conditional compilation based on LLVM_VERSION_MAJOR ensures forward and backward compatibility
- Located in the LLVM JIT emit header file, indicating it's a core utility for LLVM code generation
- The function is heavily used throughout PostgreSQL's JIT compilation for tuple deformation and expression evaluation

## Simplified Source

```c
static inline LLVMValueRef
l_gep(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, LLVMValueRef *indices, int32 nindices, const char *name)
{
    // Version-compatible Get Element Pointer instruction
    // LLVM 16+ requires explicit type parameter
#if LLVM_VERSION_MAJOR < 16
    return LLVMBuildGEP(b, v, indices, nindices, name);
#else
    return LLVMBuildGEP2(b, t, v, indices, nindices, name);
#endif
}
```

This wrapper provides version-compatible GEP (Get Element Pointer) operations for computing addresses of array elements and struct members. It handles the API differences between LLVM versions automatically.