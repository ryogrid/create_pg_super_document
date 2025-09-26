# llvm_create_jit_instance

## Location
[src/backend/jit/llvm/llvmjit.c:1312-1363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1312-L1363)

## Overview
A static function that creates and configures a complete LLVM LLJIT (Lazy Layer JIT) instance with symbol resolution capabilities for PostgreSQL's JIT compilation infrastructure.

## Definition
```c
static LLVMOrcLLJITRef llvm_create_jit_instance(LLVMTargetMachineRef tm)
```

## Detailed Description
This function creates a fully configured LLVM LLJIT instance that serves as the core JIT compilation engine for PostgreSQL. The function performs several critical setup operations:

1. **LLJIT Builder Configuration**: Creates an LLJIT builder and configures it with the provided target machine, which defines the target architecture and compilation settings.

2. **Object Layer Setup**: Registers a custom object layer creator (llvm_create_object_layer) that handles the loading of compiled machine code with debugging and profiling support.

3. **Error Handling**: Sets up an error reporter (llvm_log_jit_error) to handle JIT compilation errors gracefully without crashing the PostgreSQL process.

4. **Symbol Resolution**: Configures two types of symbol generators:
   - **Dynamic Library Generator**: Resolves symbols from the PostgreSQL binary and already-loaded libraries
   - **Custom Generator**: Resolves special PostgreSQL-specific symbols (like SQL callable functions) using llvm_resolve_symbols

The function transfers ownership of the target machine to the LLJIT instance, which will manage its lifecycle. This is a key function in PostgreSQL's JIT infrastructure, creating the execution environment for compiled expressions and functions.

## Parameters / Member Variables
- `tm`: Target machine reference that defines the compilation target architecture and settings (ownership transferred to LLJIT)

## Dependencies
- Functions called/Symbols referenced:
  - LLVMOrcCreateLLJITBuilder
  - LLVMOrcJITTargetMachineBuilderCreateFromTargetMachine
  - LLVMOrcLLJITBuilderSetJITTargetMachineBuilder
  - LLVMOrcLLJITBuilderSetObjectLinkingLayerCreator
  - [llvm_create_object_layer](llvm_create_object_layer.md) (custom object layer creator)
  - LLVMOrcCreateLLJIT
  - [llvm_error_message](llvm_error_message.md) (error message conversion)
  - LLVMOrcExecutionSessionSetErrorReporter
  - [llvm_log_jit_error](llvm_log_jit_error.md) (error callback)
  - LLVMOrcCreateDynamicLibrarySearchGeneratorForProcess
  - LLVMOrcCreateCustomCAPIDefinitionGenerator
  - [llvm_resolve_symbols](llvm_resolve_symbols.md) (custom symbol resolver)
- Called from (representative examples):
  - [llvm_session_initialize](llvm_session_initialize.md) (during JIT session setup)

## Notes and Other Information
- This is a static function local to llvmjit.c
- Transfers ownership of the target machine to the LLJIT instance
- Sets up comprehensive error handling to prevent JIT failures from crashing PostgreSQL
- Configures both standard dynamic library symbol resolution and PostgreSQL-specific symbol resolution
- The function includes LLVM version-specific conditional compilation for API compatibility
- Essential for creating the execution environment where JIT-compiled PostgreSQL expressions run
- Part of the lazy compilation infrastructure - code is compiled on-demand when first needed
- Symbol generators enable JIT-compiled code to call back into PostgreSQL functions and access global symbols