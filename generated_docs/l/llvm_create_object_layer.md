# llvm_create_object_layer

## Location
[src/backend/jit/llvm/llvmjit.c:1275-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1275-L1311)

## Overview
A static function that creates and configures an LLVM ORC object layer with optional debugging and profiling support for JIT compilation.

## Definition
```c
static LLVMOrcObjectLayerRef llvm_create_object_layer(void *Ctx, LLVMOrcExecutionSessionRef ES, const char *Triple)
```

## Detailed Description
This function creates a custom LLVM ORC (On-Request-Compilation) object layer that serves as the foundation for JIT compilation in PostgreSQL. The object layer is responsible for linking and loading compiled machine code objects into memory.

The function creates either an RTDyld object linking layer with a safe section memory manager (when using LLVM backport) or a standard section memory manager, depending on compile-time configuration. This layer handles the low-level details of loading compiled code into executable memory.

Additionally, the function conditionally registers event listeners for debugging and profiling support:
- GDB registration listener: Enables debugging of JIT-compiled code with GDB
- Perf JIT event listener: Enables profiling of JIT-compiled code with Linux perf tools

The function is a callback used by LLVM's JIT stack creation process and is essential for setting up the execution environment for compiled functions.

## Parameters / Member Variables
- `Ctx`: Context pointer (unused in current implementation)
- `ES`: LLVM ORC execution session reference for the JIT stack
- `Triple`: Target triple string describing the target architecture (unused in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager/LLVMOrcCreateRTDyldObjectLinkingLayerWithSectionMemoryManager
  - LLVMCreateGDBRegistrationListener (conditional)
  - LLVMCreatePerfJITEventListener (conditional)
  - LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener (conditional)
  - USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER (compile-time flag)
- Called from (representative examples):
  - [llvm_create_jit_instance](llvm_create_jit_instance.md) (as object layer creation callback)

## Notes and Other Information
- This is a static function local to llvmjit.c
- Supports conditional compilation for different LLVM memory manager backends
- GDB debugging support requires HAVE_DECL_LLVMCREATEGDBREGISTRATIONLISTENER
- Perf profiling support requires HAVE_DECL_LLVMCREATEPERFJITEVENTLISTENER  
- Event listener registration depends on runtime configuration variables (jit_debugging_support, jit_profiling_support)
- Essential component of PostgreSQL's LLVM JIT infrastructure
- Provides the foundation for loading and executing JIT-compiled PostgreSQL functions

## Simplified Source

```c
static LLVMOrcObjectLayerRef
llvm_create_object_layer(void *Ctx, LLVMOrcExecutionSessionRef ES, const char *Triple)
{
    // Create object layer with appropriate memory manager
#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
    LLVMOrcObjectLayerRef objlayer =
        LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(ES);
#else
    LLVMOrcObjectLayerRef objlayer =
        LLVMOrcCreateRTDyldObjectLinkingLayerWithSectionMemoryManager(ES);
#endif

    // Add GDB debugging support if available and enabled
    if (jit_debugging_support) {
        LLVMJITEventListenerRef listener = LLVMCreateGDBRegistrationListener();
        LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener(objlayer, listener);
    }

    // Add Perf profiling support if available and enabled
    if (jit_profiling_support) {
        LLVMJITEventListenerRef listener = LLVMCreatePerfJITEventListener();
        LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener(objlayer, listener);
    }

    return objlayer;
}
```