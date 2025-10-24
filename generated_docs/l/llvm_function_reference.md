# llvm_function_reference

## Location
[src/backend/jit/llvm/llvmjit.c:573-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L573-L635)

## Overview
Returns a callable LLVMValueRef for a PostgreSQL function referenced by FunctionCallInfo, handling both internal functions and external library functions.

## Definition
```c
LLVMValueRef llvm_function_reference(LLVMJitContext *context, 
                                      LLVMBuilderRef builder,
                                      LLVMModuleRef mod, 
                                      FunctionCallInfo fcinfo)
```

## Detailed Description
This function creates callable LLVM function references for PostgreSQL functions based on their FunctionCallInfo. It handles three distinct categories of functions:

1. **External functions in loadable libraries**: Functions from shared libraries are named with the pattern "pgextern.{modname}.{basename}"
2. **Internal PostgreSQL functions**: Built-in functions use their basename directly  
3. **Unknown functions**: Functions without symbol information are handled by creating global constants containing function pointers, named "pgoidextern.{oid}"

The function implements caching by checking if the function already exists in the module before creating new declarations. For unknown functions, it creates a global constant initialized with the function pointer and returns a loaded value, making the generated IR more readable while maintaining functionality.

## Parameters / Member Variables
- `context`: LLVM JIT compilation context
- `builder`: LLVM instruction builder for generating code
- `mod`: Target LLVM module to add the function to
- `fcinfo`: PostgreSQL function call information containing function OID and address

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_symbol](../f/fmgr_symbol.md) (PostgreSQL function manager)
  - [psprintf](../p/psprintf.md) (PostgreSQL string formatting)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - LLVMGetNamedGlobal (LLVM C API)
  - LLVMGetNamedFunction (LLVM C API)
  - LLVMAddFunction (LLVM C API)
  - LLVMAddGlobal (LLVM C API)
  - LLVMSetInitializer (LLVM C API)
  - LLVMSetGlobalConstant (LLVM C API)
  - LLVMSetLinkage (LLVM C API)
  - LLVMSetUnnamedAddr (LLVM C API)
  - [l_load](l_load.md) (PostgreSQL LLVM utility)
  - [l_ptr_const](l_ptr_const.md) (PostgreSQL LLVM utility)
  - [AttributeTemplate](../A/AttributeTemplate.md) (global function type template)
  - TypePGFunction (PostgreSQL function type)

- Called from (representative examples):
  - [llvm_compile_expr](llvm_compile_expr.md) (in llvmjit_expr.c)
  - [BuildV1Call](../B/BuildV1Call.md) (in llvmjit_expr.c)

## Notes and Other Information
- Located in src/backend/jit/llvm/llvmjit.c:573-635
- Handles three distinct function reference patterns for maximum compatibility
- Implements function caching to avoid duplicate declarations
- Creates readable IR by using meaningful names for unknown functions
- Uses private linkage and unnamed addresses for global constants to optimize generated code
- Essential for PostgreSQL's function call compilation in the LLVM JIT system
- Bridges the gap between PostgreSQL's function management and LLVM's function representation

## Simplified Source

```c
LLVMValueRef
llvm_function_reference(LLVMJitContext *context,
                        LLVMBuilderRef builder,
                        LLVMModuleRef mod,
                        FunctionCallInfo fcinfo)
{
    char *modname, *basename, *funcname;

    // Get function symbol information
    fmgr_symbol(fcinfo->flinfo->fn_oid, &modname, &basename);

    if (modname != NULL && basename != NULL) {
        // External function in loadable library
        funcname = psprintf("pgextern.%s.%s", modname, basename);
    }
    else if (basename != NULL) {
        // Internal PostgreSQL function
        funcname = pstrdup(basename);
    }
    else {
        // Unknown function - create global constant with function pointer
        funcname = psprintf("pgoidextern.%u", fcinfo->flinfo->fn_oid);

        LLVMValueRef v_fn = LLVMGetNamedGlobal(mod, funcname);
        if (v_fn != 0)
            return l_load(builder, TypePGFunction, v_fn, "");

        // Create global constant containing function pointer
        LLVMValueRef v_fn_addr = l_ptr_const(fcinfo->flinfo->fn_addr, TypePGFunction);
        v_fn = LLVMAddGlobal(mod, TypePGFunction, funcname);
        LLVMSetInitializer(v_fn, v_fn_addr);
        LLVMSetGlobalConstant(v_fn, true);
        LLVMSetLinkage(v_fn, LLVMPrivateLinkage);
        LLVMSetUnnamedAddr(v_fn, true);

        return l_load(builder, TypePGFunction, v_fn, "");
    }

    // Check if function already exists
    LLVMValueRef v_fn = LLVMGetNamedFunction(mod, funcname);
    if (v_fn != 0)
        return v_fn;

    // Add new function declaration
    return LLVMAddFunction(mod, funcname, LLVMGetFunctionType(AttributeTemplate));
}
```