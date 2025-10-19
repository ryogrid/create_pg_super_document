# l_load_gep1

## Location
[src/include/jit/llvmjit_emit.h:163-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L163-L168)

## Overview
A convenience function that combines single-index pointer arithmetic (GEP) with loading, providing a high-level interface for accessing array elements or pointer offsets in LLVM IR.

## Definition
```c
static inline LLVMValueRef l_load_gep1(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, LLVMValueRef idx, const char *name)
```

## Detailed Description
The `l_load_gep1` function is a composite utility that simplifies the common pattern of performing single-index pointer arithmetic followed by loading the value at the computed address. It combines two fundamental operations: computing a pointer offset using GetElementPtr with a single index (using `l_gep`) and then loading the value from that pointer (using `l_load`). This abstraction is particularly useful for array access and pointer arithmetic operations in LLVM IR generation.

The "1" in the function name indicates it performs a single-index GEP operation, making it ideal for linear array access or stepping through contiguous memory structures.

## Parameters / Member Variables
- `b`: LLVM builder reference used to insert the instructions
- `t`: LLVM type reference representing the element type being accessed
- `v`: LLVM value reference representing the base pointer
- `idx`: LLVM value reference representing the index for the GEP operation
- `name`: String name for the resulting LLVM value (for debugging/readability)

## Dependencies
- Functions called/Symbols referenced:
  - [l_gep](l_gep.md): Used to compute the pointer arithmetic with the single index
  - [l_load](l_load.md): Used to load the value from the computed pointer
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (in llvmjit_deform.c)
  - [llvm_compile_expr](llvm_compile_expr.md) (extensively in llvmjit_expr.c)

## Notes and Other Information
- This function is part of PostgreSQL's JIT compilation infrastructure
- Specifically designed for single-index operations, making it more efficient than the general `l_gep` for array access
- Commonly used for accessing elements in PostgreSQL arrays, stepping through memory buffers, and pointer arithmetic
- The combination of single-index GEP + load is a frequent pattern in PostgreSQL's JIT code for array and buffer operations
- Essential for implementing operations on PostgreSQL's internal data structures that involve indexed access
- Provides type safety by ensuring the load operation uses the correct element type
- More specialized than `l_load_struct_gep` which is for struct members, while this is for array-like access patterns

## Simplified Source

```c
// Convenience function: compute pointer + index, then load value
static inline LLVMValueRef
l_load_gep1(LLVMBuilderRef b, LLVMTypeRef t, LLVMValueRef v, LLVMValueRef idx, const char *name)
{
    // Step 1: Calculate pointer with single index offset
    LLVMValueRef ptr = l_gep(b, t, v, &idx, 1, "");

    // Step 2: Load value from calculated pointer
    return l_load(b, t, ptr, name);
}
```