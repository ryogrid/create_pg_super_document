# l_funcvaluep

## Location
[src/include/jit/llvmjit_emit.h:295-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L295-L320)

## Overview
Returns a pointer to the datum value of a specific argument in a PostgreSQL function call information structure, used in LLVM JIT compilation.

## Definition
```c
static inline LLVMValueRef
l_funcvaluep(LLVMBuilderRef b, LLVMValueRef v_fcinfo, size_t argno)
```

## Detailed Description
This function is part of PostgreSQL's LLVM JIT compilation infrastructure. It provides access to the actual datum value of a specific function argument within the FunctionCallInfoData structure. Similar to l_funcnullp, this function navigates through the LLVM IR representation of PostgreSQL's data structures, but instead returns a pointer to the datum field rather than the nullness indicator.

The function performs the same initial navigation as l_funcnullp but accesses the datum field:
1. Gets a pointer to the args array in the FunctionCallInfoData structure
2. Indexes into the array to get the specific argument (argno)
3. Returns a pointer to the datum field of that argument's NullableDatum structure

## Parameters / Member Variables
- `b`: LLVM builder reference used for generating LLVM IR instructions
- `v_fcinfo`: LLVM value representing a pointer to the FunctionCallInfoData structure
- `argno`: Zero-based index of the argument whose datum pointer is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [l_struct_gep](l_struct_gep.md) (called 3 times for structure field access)
- Called from (representative examples):
  - llvm_compile_expr (in src/backend/jit/llvm/llvmjit_expr.c at lines 1361, 1413, 1611, 2492)
  - [l_funcvalue](l_funcvalue.md)

## Notes and Other Information
- This is an inline function defined in the LLVM JIT emit header file
- Companion function to l_funcnullp - they access different fields of the same NullableDatum structure
- Returns a pointer to the datum field, not the actual datum value itself
- Essential for JIT-compiled code to access function argument values efficiently
- The datum field contains the actual PostgreSQL Datum value for the argument
- Used extensively in expression compilation where argument values need to be accessed