# l_int32_const

## Location
[src/include/jit/llvmjit_emit.h:66-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L66-L74)

## Overview
A static inline utility function that creates an LLVM constant for 32-bit integer values in JIT compilation.

## Definition
```c
static inline LLVMValueRef
l_int32_const(LLVMContextRef lc, int32 i)
```

## Detailed Description
This function creates an LLVM constant value representing a 32-bit signed integer. It takes a PostgreSQL `int32` value and converts it into an LLVM constant that can be embedded in generated LLVM IR code. The function uses the LLVM context to create the appropriate 32-bit integer type and then creates a constant value of that type.

This is part of PostgreSQL's JIT compilation infrastructure and is widely used during both tuple slot deformation operations and expression compilation where 32-bit integer constants need to be embedded in the generated code. It's particularly important for representing larger numeric values, array indices, function arguments, and various PostgreSQL internal identifiers that require 32-bit precision.

## Parameters / Member Variables
- `lc`: The LLVM context in which to create the constant
- `i`: The 32-bit signed integer value to convert to an LLVM constant

## Dependencies
- Functions called/Symbols referenced:
  - LLVMConstInt (LLVM C API function)
  - LLVMInt32TypeInContext (LLVM C API function)
  - int32 (PostgreSQL type alias for signed 32-bit integer)
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (in llvmjit_deform.c, used for tuple slot deformation operations)
  - [llvm_compile_expr](llvm_compile_expr.md) (in llvmjit_expr.c, used extensively for expression compilation with 15+ call sites)

## Notes and Other Information
- This is a header-only inline function defined in src/include/jit/llvmjit_emit.h
- Part of a family of integer constant creation functions (l_int8_const, l_int16_const, l_int32_const)
- The `false` parameter to LLVMConstInt indicates the value should be treated as signed
- Most heavily used in expression compilation (15+ call sites in llvm_compile_expr)
- Also used in tuple deformation for larger numeric values
- Essential for generating efficient JIT code that manipulates PostgreSQL's internal data representations
- Common for representing function IDs, array indices, larger offsets, and complex numeric computations
- The 32-bit variant is the most commonly used among the integer constant functions due to its versatility