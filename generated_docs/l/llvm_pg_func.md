# llvm_pg_func

## Location
src/backend/jit/llvm/llvmjit.c: 497 - 524

## Overview
Returns a function declaration for a function referenced in llvmjit_types.c, adding it to the specified LLVM module if necessary.

## Definition
```c
LLVMValueRef llvm_pg_func(LLVMModuleRef mod, const char *funcname)
```

## Detailed Description
This function creates or retrieves LLVM function declarations in a target module based on function prototypes defined in llvmjit_types.c. It serves as a bridge for making functions discovered via llvm_create_types() available to the module currently being worked on. The function implements caching by checking if the function already exists in the target module before creating a new declaration.

When adding a function to the module, it copies both the function type and attributes from the source function in llvm_types_module to ensure consistent behavior. This is crucial for maintaining compatibility between different LLVM modules within the PostgreSQL JIT system.

## Parameters / Member Variables
- `mod`: The LLVM module to add the function declaration to
- `funcname`: The name of the function to look up and add

## Dependencies
- Functions called/Symbols referenced:
  - LLVMGetNamedFunction (LLVM C API)
  - LLVMAddFunction (LLVM C API)
  - LLVMGetFunctionType (LLVM C API)
  - llvm_copy_attributes (PostgreSQL JIT utility)
  - elog (PostgreSQL logging)
  - llvm_types_module (global LLVM module)

- Called from (representative examples):
  - slot_compile_deform (in llvmjit_deform.c)
  - llvm_compile_expr (in llvmjit_expr.c)
  - build_EvalXFuncInt (in llvmjit_expr.c)

## Notes and Other Information
- Located in src/backend/jit/llvm/llvmjit.c:497-524
- Implements function declaration caching to avoid duplicate additions
- Essential for cross-module function referencing in PostgreSQL's LLVM JIT system
- Copies function attributes to maintain behavioral consistency
- Throws ERROR if the requested function is not found in llvmjit_types.c
- Used extensively throughout the JIT compilation infrastructure for accessing PostgreSQL runtime functions