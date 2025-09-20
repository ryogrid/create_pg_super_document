# l_struct_gep

## Location
[src/include/jit/llvmjit_emit.h:108-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L108-L117)

## Overview
Provides a version-compatible wrapper for LLVM's struct GEP (Get Element Pointer) instruction to access struct member elements within PostgreSQL's JIT compilation system.

## Definition

```c
static inline LLVMValueRef
l_struct_gep(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, int32 idx, const char *name)
```
## Detailed Description
This function serves as a compatibility wrapper for LLVM's struct GEP (Get Element Pointer) operations, which are used to generate addresses of struct members. The function automatically handles the API differences between LLVM versions - for LLVM versions prior to 16, it uses , while for version 16 and later, it uses  which requires an explicit type parameter. This abstraction allows PostgreSQL's JIT code to work across different LLVM versions without conditional compilation scattered throughout the codebase. The function is essential for accessing members of complex data structures in JIT-compiled code.

## Parameters / Member Variables
- : LLVM builder reference used to construct the instruction
- : LLVM type reference representing the struct type (used in LLVM 16+)
- : LLVM value reference to the struct instance
- : 32-bit integer index of the struct member to access
- : Name for the generated instruction (currently unused, empty string is passed)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMBuildStructGEP (LLVM C API function, for LLVM < 16)
  - LLVMBuildStructGEP2 (LLVM C API function, for LLVM >= 16)
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (multiple locations in src/backend/jit/llvm/llvmjit_deform.c)
  - llvm_compile_expr (multiple locations in src/backend/jit/llvm/llvmjit_expr.c)
  - [l_load_struct_gep](l_load_struct_gep.md) (src/include/jit/llvmjit_emit.h:155)
  - [l_funcnullp](l_funcnullp.md) and l_funcvaluep (utility functions in src/include/jit/llvmjit_emit.h)

## Notes and Other Information
- Critical for LLVM version compatibility across PostgreSQL deployments
- The  parameter is currently unused (empty string passed) but maintained for API consistency
- Essential for struct member access in JIT-compiled expressions and tuple deformation
- Used extensively in both expression compilation and tuple slot operations
- The conditional compilation ensures optimal performance while maintaining compatibility