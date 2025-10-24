# load_return_type

## Location
[src/backend/jit/llvm/llvmjit.c:1052-1071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1052-L1071)

## Overview
Helper function that extracts and returns the return type of a named function from an LLVM module for use in type system setup.

## Definition

```c
static LLVMTypeRef
load_return_type(LLVMModuleRef mod, const char *name)
```
## Detailed Description
This utility function serves as a helper for , providing a convenient way to extract the return type of functions that are already defined in an LLVM module. The function:

1. **Function Lookup**: Uses  to locate the specified function by name in the module
2. **Error Handling**: Validates that the function exists and reports an error if not found
3. **Type Extraction**: Calls  (implemented in llvmjit_wrap.cpp) to extract the function's return type
4. **Return Type Delivery**: Returns the LLVM type reference for use in PostgreSQL's LLVM type system setup

This function is particularly useful when setting up PostgreSQL's internal LLVM type mappings by examining the return types of existing C functions that have been compiled into LLVM IR.

## Parameters / Member Variables
- `mod`: LLVMModuleRef pointing to the LLVM module containing the target function
- `*name`: const char* specifying the name of the function whose return type should be extracted
## Dependencies
- Functions called/Symbols referenced:
  - LLVMGetNamedFunction (LLVM API function)
  - LLVMGetFunctionReturnType (wrapper function in llvmjit_wrap.cpp)
  - elog (PostgreSQL error reporting)
- Called from (representative examples):
  - [llvm_create_types](llvm_create_types.md)

## Notes and Other Information
- The function is specifically designed as a helper for type system initialization
- Error handling ensures that missing functions are caught early in the setup process
- The comment indicates that  returns a pointer to the function, not the function itself
-  is implemented in llvmjit_wrap.cpp, suggesting it may be a C++ wrapper around LLVM's C++ API
- The function is simple but essential for PostgreSQL's LLVM JIT type system setup
- Return type information is crucial for generating correct LLVM IR that matches PostgreSQL's internal type system

## Simplified Source

```c
static LLVMTypeRef load_return_type(LLVMModuleRef mod, const char *name) {
    // Get function from module by name
    LLVMValueRef function = LLVMGetNamedFunction(mod, name);
    if (!function)
        elog(ERROR, "function %s is unknown", name);

    // Extract and return the function's return type
    return LLVMGetFunctionReturnType(function);
}
```