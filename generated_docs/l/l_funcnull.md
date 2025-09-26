# l_funcnull

## Location
[src/include/jit/llvmjit_emit.h:321-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L321-L329)

## Overview
Returns the nullness value (as an LLVM value) of a specific argument in a PostgreSQL function call, used in LLVM JIT compilation.

## Definition
```c
static inline LLVMValueRef
l_funcnull(LLVMBuilderRef b, LLVMValueRef v_fcinfo, size_t argno)
```

## Detailed Description
This function is a convenience wrapper around l_funcnullp that not only gets a pointer to the nullness indicator but also loads the actual boolean value from that location. It combines the pointer access functionality of l_funcnullp with a load operation to retrieve the actual nullness state of a function argument.

The function is a simple one-liner that:
1. Calls l_funcnullp to get a pointer to the nullness field
2. Uses l_load to dereference that pointer and load the boolean value
3. Returns the loaded boolean value as an LLVM value

This provides a more convenient interface when the actual nullness value is needed rather than just a pointer to it.

## Parameters / Member Variables
- `b`: LLVM builder reference used for generating LLVM IR instructions
- `v_fcinfo`: LLVM value representing a pointer to the FunctionCallInfoData structure
- `argno`: Zero-based index of the argument whose nullness value is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [l_load](l_load.md) (for loading the boolean value from the pointer)
  - [l_funcnullp](l_funcnullp.md) (for getting the pointer to the nullness field)
- Called from (representative examples):
  - [llvm_compile_expr](llvm_compile_expr.md) (in src/backend/jit/llvm/llvmjit_expr.c at lines 649, 1471, 1474, 1569, 1570, 1712, 1713, 2156)

## Notes and Other Information
- This is an inline function defined in the LLVM JIT emit header file
- Higher-level convenience function compared to l_funcnullp
- Returns the actual boolean nullness value, not a pointer
- Widely used in expression compilation for null checking logic
- The returned value can be used directly in LLVM conditional branches and comparisons
- Essential for implementing PostgreSQL's three-valued logic (true/false/null) in JIT-compiled code