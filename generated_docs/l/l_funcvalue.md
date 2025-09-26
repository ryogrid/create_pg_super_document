# l_funcvalue

## Location
[src/include/jit/llvmjit_emit.h:330-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L330-L336)

## Overview
Returns the datum value (as an LLVM value) of a specific argument in a PostgreSQL function call, used in LLVM JIT compilation.

## Definition
```c
static inline LLVMValueRef
l_funcvalue(LLVMBuilderRef b, LLVMValueRef v_fcinfo, size_t argno)
```

## Detailed Description
This function is a convenience wrapper around l_funcvaluep that not only gets a pointer to the datum value but also loads the actual datum from that location. Similar to how l_funcnull wraps l_funcnullp, this function combines the pointer access functionality of l_funcvaluep with a load operation to retrieve the actual datum value of a function argument.

The function is a simple one-liner that:
1. Calls l_funcvaluep to get a pointer to the datum field
2. Uses l_load to dereference that pointer and load the datum value (as TypeSizeT)
3. Returns the loaded datum value as an LLVM value

This provides a more convenient interface when the actual datum value is needed rather than just a pointer to it.

## Parameters / Member Variables
- `b`: LLVM builder reference used for generating LLVM IR instructions
- `v_fcinfo`: LLVM value representing a pointer to the FunctionCallInfoData structure
- `argno`: Zero-based index of the argument whose datum value is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [l_load](l_load.md) (for loading the datum value from the pointer, using TypeSizeT type)
  - [l_funcvaluep](l_funcvaluep.md) (for getting the pointer to the datum field)
- Called from (representative examples):
  - [llvm_compile_expr](llvm_compile_expr.md) (in src/backend/jit/llvm/llvmjit_expr.c at line 1566)

## Notes and Other Information
- This is an inline function defined in the LLVM JIT emit header file
- Higher-level convenience function compared to l_funcvaluep
- Returns the actual datum value, not a pointer to it
- Less frequently used compared to l_funcnull, as many operations work with datum pointers directly
- The datum is loaded as TypeSizeT, which corresponds to PostgreSQL's Datum type
- Essential for accessing actual argument values in JIT-compiled expressions
- Companion function to l_funcnull - together they provide complete access to argument nullness and values