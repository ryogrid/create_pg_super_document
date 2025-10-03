# l_int64_const

## Location
[src/include/jit/llvmjit_emit.h:75-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L75-L83)

## Overview
Creates an LLVM constant integer value of 64-bit signed integer type for use in LLVM IR code generation within PostgreSQL's JIT compilation system.

## Definition

```c
static inline LLVMValueRef
l_int64_const(LLVMContextRef lc, int64 i)
```
## Detailed Description
This is a utility function that wraps LLVM's  function to create 64-bit signed integer constants in LLVM IR. It's part of PostgreSQL's JIT (Just-In-Time) compilation infrastructure, specifically used for generating LLVM intermediate representation code. The function takes a 64-bit signed integer value and creates an LLVM constant value that can be used in LLVM IR instructions. The function is implemented as a static inline function for performance, as it's likely to be called frequently during JIT compilation.

## Parameters / Member Variables
- `lc`: LLVM context reference that provides the execution context for LLVM operations
- `i`: The 64-bit signed integer value to be converted into an LLVM constant
## Dependencies
- Functions called/Symbols referenced:
  - LLVMConstInt (LLVM C API function)
  - LLVMInt64TypeInContext (LLVM C API function)
- Called from (representative examples):
  - [BuildV1Call](../B/BuildV1Call.md) (src/backend/jit/llvm/llvmjit_expr.c:2739, 2743)

## Notes and Other Information
- This function is part of the LLVM JIT emission utilities defined in llvmjit_emit.h
- The function uses the LLVM C API to create integer constants
- The third parameter  in LLVMConstInt indicates the value is not sign-extended
- This is a convenience wrapper that simplifies creating 64-bit integer constants in PostgreSQL's JIT code generation