# llvm_pg_var_func_type

## Location
[src/backend/jit/llvm/llvmjit.c:475-496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L475-L496)

## Overview
Returns the LLVM function type of a variable defined in llvmjit_types.c, ensuring function type synchronization between C code and JIT compiled code.

## Definition

```c
LLVMTypeRef
llvm_pg_var_func_type(const char *varname)
```
## Detailed Description
This function retrieves the LLVM function type for a named function variable from the llvmjit_types.c module. It serves as a bridge between PostgreSQL's C code and LLVM JIT compilation by providing type information needed for generating compatible JIT code. The function looks up a named function in the global llvm_types_module and extracts its type information.

The function ensures type safety by verifying that the requested function exists in the types module, throwing an error if the function is not found. This prevents runtime type mismatches between C and JIT code.

## Parameters / Member Variables
- `varname`: The name of the function variable to look up in llvmjit_types.c

## Dependencies
- Functions called/Symbols referenced:
  - LLVMGetNamedFunction (LLVM C API)
  - LLVMGetFunctionType (LLVM C API) 
  - elog (PostgreSQL logging)
  - llvm_types_module (global LLVM module)

- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md) (in llvmjit_deform.c)
  - llvm_compile_expr (in llvmjit_expr.c)

## Notes and Other Information
- Located in src/backend/jit/llvm/llvmjit.c:475-496
- Essential for maintaining type compatibility between C and JIT compiled code
- Throws ERROR if the requested function is not found in llvmjit_types.c
- Used extensively in expression compilation and tuple deformation JIT code
- Part of PostgreSQL's LLVM JIT compilation infrastructure