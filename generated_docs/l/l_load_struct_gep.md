# l_load_struct_gep

## Location
src/include/jit/llvmjit_emit.h: 151 - 162

## Overview
A convenience function that combines struct member access (GEP) with loading the value, providing a high-level interface for reading struct fields in LLVM IR.

## Definition
```c
static inline LLVMValueRef l_load_struct_gep(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, int32 idx, const char *name)
```

## Detailed Description
The `l_load_struct_gep` function is a composite utility that simplifies the common pattern of accessing and loading a struct member in LLVM IR. It combines two fundamental operations: getting a pointer to a struct member (using `l_struct_gep`) and then loading the value from that pointer (using `l_load`). This abstraction eliminates the need to manually chain these operations throughout the codebase, making struct member access more concise and readable.

The function automatically determines the correct type for the load operation by querying the struct type definition, ensuring type safety in the generated LLVM IR.

## Parameters / Member Variables
- `b`: LLVM builder reference used to insert the instructions
- `t`: LLVM type reference representing the struct type
- `v`: LLVM value reference representing a pointer to the struct instance
- `idx`: Zero-based index of the struct member to access
- `name`: String name for the resulting LLVM value (for debugging/readability)

## Dependencies
- Functions called/Symbols referenced:
  - l_load: Used to load the value from the computed pointer
  - l_struct_gep: Used to get a pointer to the struct member
  - LLVMStructGetTypeAtIndex: LLVM API to get the type of the struct member
- Called from (representative examples):
  - slot_compile_deform (extensively in llvmjit_deform.c)
  - llvm_compile_expr (extensively in llvmjit_expr.c)

## Notes and Other Information
- This function is part of PostgreSQL's JIT compilation infrastructure
- Heavily used for accessing PostgreSQL data structures like TupleTableSlot, ExprContext, and other runtime structures
- Provides a clean abstraction over the common pattern of struct member access in LLVM
- The combination of GEP + load is so common in PostgreSQL's JIT code that this helper significantly reduces code duplication
- Essential for implementing tuple deformation and expression evaluation where struct members need to be frequently accessed
- The automatic type resolution using `LLVMStructGetTypeAtIndex` ensures the load operation uses the correct type, preventing type mismatches in the generated IR