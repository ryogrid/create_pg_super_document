# BuildV1Call

## Location
[src/backend/jit/llvm/llvmjit_expr.c:2704-2754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit_expr.c#L2704-L2754)

## Overview
Generates LLVM IR code to call a PostgreSQL function through the FunctionCallInfo interface, handling function resolution, null flag management, and optimization hints.

## Definition

```c
struct_gep(b,
									StructFunctionCallInfoData,
									v_fcinfo,
									FIELDNO_FUNCTIONCALLINFODATA_ISNULL,
									"v_fcinfo_isnull");
```
## Detailed Description
This function is a crucial component of PostgreSQL's LLVM JIT compilation system, responsible for generating LLVM IR code that calls PostgreSQL functions using the version-1 calling convention. The function performs several important tasks:

1. **Function Reference Resolution**: Uses llvm_function_reference to obtain an LLVM function reference for the PostgreSQL function specified in the FunctionCallInfo structure. This handles both internal PostgreSQL functions and dynamically loaded functions.

2. **Function Call Setup**: Creates the necessary LLVM IR to set up the function call, including:
   - Creating a pointer to the FunctionCallInfo structure
   - Getting a pointer to the isnull field for result null flag management
   - Initializing the isnull field to false before the call

3. **Function Invocation**: Generates the actual LLVM function call using the AttributeTemplate function signature, passing the FunctionCallInfo structure as the argument.

4. **Result Processing**: Optionally loads the result's null flag from the isnull field and stores it in the provided output parameter.

5. **Lifetime Management**: Adds LLVM lifetime-end annotations for the function arguments and isnull field to provide optimization hints. This signals to LLVM that these memory locations don't need to be preserved after the function call, enabling better inlining and optimization.

The function is designed to seamlessly integrate PostgreSQL's C function calling convention with LLVM's optimization framework, enabling high-performance execution of database functions while maintaining full compatibility with PostgreSQL's function interface.

## Parameters / Member Variables
- : The LLVM JIT context containing compilation state and function caches
- : The LLVM IR builder for generating instructions
- : The LLVM module where the function call should be generated
- : The FunctionCallInfo structure containing function metadata and arguments
- : Optional output parameter to receive the LLVM value representing the function result's null flag

## Dependencies
- Functions called/Symbols referenced:
  - [llvm_function_reference](../l/llvm_function_reference.md)
  - [l_ptr_const](../l/l_ptr_const.md)
  - [l_struct_gep](../l/l_struct_gep.md)
  - [l_sbool_const](../l/l_sbool_const.md)
  - [AttributeTemplate](../A/AttributeTemplate.md)
  - [l_call](../l/l_call.md)
  - [l_load](../l/l_load.md)
  - [create_LifetimeEnd](../c/create_LifetimeEnd.md)
  - [NullableDatum](../N/NullableDatum.md)
  - [l_int64_const](../l/l_int64_const.md)
- Called from (representative examples):
  - [llvm_compile_expr](../l/llvm_compile_expr.md) (multiple call sites for different expression types)

## Notes and Other Information
- Returns an LLVMValueRef representing the function's return value
- The function is static and only used within the LLVM expression compilation system
- Uses the version-1 PostgreSQL function calling convention
- The lifetime-end annotations are crucial for enabling LLVM optimizations, particularly function inlining
- Properly handles the PostgreSQL function null flag protocol by initializing isnull to false and returning the final state
- The function signature uses AttributeTemplate to ensure consistent calling conventions