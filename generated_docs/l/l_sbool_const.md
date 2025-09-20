# l_sbool_const

## Location
[src/include/jit/llvmjit_emit.h:93-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L93-L101)

## Overview
Creates an LLVM constant boolean value suitable for storage operations (such as global variables and struct members) within PostgreSQL's JIT compilation system.

## Definition

```c
static inline LLVMValueRef
l_sbool_const(bool i)
```
## Detailed Description
This utility function creates LLVM constant boolean values specifically designed for storage contexts. Unlike boolean values used for conditional operations, storage booleans need to match the memory representation expected by PostgreSQL's data structures. The function uses , which represents the LLVM type corresponding to PostgreSQL's storage representation of boolean values. It converts the C boolean parameter to an integer representation suitable for LLVM IR generation. This function is extensively used throughout expression compilation where boolean values need to be stored or compared against stored boolean values.

## Parameters / Member Variables
- : The boolean value to be converted into an LLVM constant suitable for storage

## Dependencies
- Functions called/Symbols referenced:
  - LLVMConstInt (LLVM C API function)
  - TypeStorageBool (global LLVM type reference)
- Called from (representative examples):
  - llvm_compile_expr (extensively used throughout src/backend/jit/llvm/llvmjit_expr.c)
  - BuildV1Call (src/backend/jit/llvm/llvmjit_expr.c:2724)

## Notes and Other Information
- Specifically designed for storage contexts, distinguishing it from boolean values used in conditional logic
- The function casts the boolean to int before passing to LLVMConstInt
- Uses TypeStorageBool which must match PostgreSQL's internal boolean storage representation
- The third parameter  in LLVMConstInt indicates the value is not sign-extended
- Critical for ensuring proper boolean handling in data structures and storage operations within JIT-compiled code