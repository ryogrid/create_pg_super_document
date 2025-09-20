# l_funcnullp

## Location
[src/include/jit/llvmjit_emit.h:269-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L269-L294)

## Overview
Returns a pointer to the nullness indicator (isnull field) of a specific argument in a PostgreSQL function call information structure, used in LLVM JIT compilation.

## Definition

```c
struct_gep(b,
						  StructFunctionCallInfoData,
						  v_fcinfo,
						  FIELDNO_FUNCTIONCALLINFODATA_ARGS,
						  "");
```
## Detailed Description
This function is part of PostgreSQL's LLVM JIT compilation infrastructure. It provides a way to access the nullness indicator of a specific function argument within the FunctionCallInfoData structure. The function navigates through the LLVM IR representation of the data structures to return a pointer to the isnull field of the specified argument's NullableDatum structure.

The function performs a series of LLVM structure field accesses:
1. Gets a pointer to the args array in the FunctionCallInfoData structure
2. Indexes into the array to get the specific argument (argno)
3. Returns a pointer to the isnull field of that argument's NullableDatum

## Parameters / Member Variables
- : LLVM builder reference used for generating LLVM IR instructions
- : LLVM value representing a pointer to the FunctionCallInfoData structure
- : Zero-based index of the argument whose nullness pointer is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [l_struct_gep](l_struct_gep.md) (called 3 times for structure field access)
- Called from (representative examples):
  - llvm_compile_expr (in src/backend/jit/llvm/llvmjit_expr.c at lines 1364, 1415, 2495)
  - [l_funcnull](l_funcnull.md)

## Notes and Other Information
- This is an inline function defined in the LLVM JIT emit header file
- Used specifically for LLVM IR generation during JIT compilation of PostgreSQL expressions
- Returns a pointer to the nullness field, not the actual boolean value
- Part of the infrastructure that allows JIT-compiled code to efficiently check argument nullness
- The function assumes the FunctionCallInfoData structure layout and NullableDatum structure format