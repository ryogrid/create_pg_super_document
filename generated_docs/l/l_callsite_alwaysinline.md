# l_callsite_alwaysinline

## Location
[src/include/jit/llvmjit_emit.h:235-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/jit/llvmjit_emit.h#L235-L250)

## Overview
A utility function that marks an LLVM function call site with the "alwaysinline" attribute to force the LLVM compiler to inline the function call.

## Definition


## Detailed Description
This function applies the "alwaysinline" attribute to an LLVM function call site, which instructs the LLVM compiler to always inline the function call regardless of normal inlining heuristics. This is a more aggressive optimization directive than regular inlining hints, as it overrides LLVM's cost-benefit analysis and forces inlining even for large functions or in scenarios where inlining might normally be avoided.

The function uses LLVM's enumerated attribute system (rather than string attributes) since "alwaysinline" is a well-known LLVM attribute. This provides better performance when creating the attribute and is the preferred method for standard LLVM attributes. The attribute is attached to the function index of the call site, ensuring that this specific call will be inlined.

This is particularly valuable in PostgreSQL's JIT compilation for critical performance paths where function call overhead must be eliminated, such as in tight loops processing many tuples or frequently called utility functions.

## Parameters / Member Variables
- `f`: The LLVM value representing the function call site to be marked for forced inlining

## Dependencies
- Functions called/Symbols referenced:
  - `LLVMGetEnumAttributeKindForName`: To get the numeric ID for the "alwaysinline" attribute
  - `LLVMTypeOf`: To get the type of the function call
  - `LLVMGetTypeContext`: To extract the LLVM context from the function type
  - `LLVMCreateEnumAttribute`: To create the alwaysinline enumerated attribute
  - `LLVMAddCallSiteAttribute`: To attach the attribute to the call site
- Called from (representative examples):
  - [slot_compile_deform](../s/slot_compile_deform.md): Used when calling critical functions that must be inlined for optimal performance

## Notes and Other Information
- Uses enumerated attributes rather than string attributes for better performance
- The "alwaysinline" attribute is more aggressive than regular inlining and should be used judiciously
- Particularly useful for small, frequently called functions in performance-critical code paths
- Can potentially increase code size significantly if used on large functions or in many locations
- Used very selectively in PostgreSQL's JIT infrastructure where the performance benefit clearly outweighs the code size cost
- The forced inlining can help with subsequent optimization passes by exposing more optimization opportunities across the inlined boundary