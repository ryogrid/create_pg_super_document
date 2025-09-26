# llvm_log_jit_error

## Location
[src/backend/jit/llvm/llvmjit.c:1265-1274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1265-L1274)

## Overview
A static error logging callback function used by LLVM JIT compilation to handle and log errors that occur during the JIT process.

## Definition

```c
static void
llvm_log_jit_error(void *ctx, LLVMErrorRef error)
```
## Detailed Description
This function serves as an error callback for LLVM JIT operations, specifically designed to handle errors that cannot be thrown through the LLVM stack without causing fatal errors. Instead of throwing exceptions, it logs errors as warnings using PostgreSQL's elog system.

The function is critical for maintaining system stability during JIT compilation failures. Since errors cannot be safely propagated through LLVM's C API without risking FATAL termination of the PostgreSQL process, this callback provides a safe mechanism to capture and report JIT compilation issues.

The function is typically used during symbol resolution breakage and other JIT-related failures that occur outside normal operation. By logging at WARNING level, it ensures that JIT errors are visible for debugging while allowing the system to continue operating.

## Parameters / Member Variables
- : Context pointer (currently unused in the implementation)
- : LLVM error reference containing details about the JIT compilation failure

## Dependencies
- Functions called/Symbols referenced:
  - elog (PostgreSQL logging function)
  - [llvm_error_message](llvm_error_message.md) (converts LLVM error to string)
- Called from (representative examples):
  - [llvm_create_jit_instance](llvm_create_jit_instance.md) (registered as error callback)

## Notes and Other Information
- This is a static function local to llvmjit.c
- Uses WARNING level logging instead of ERROR to prevent process termination
- Errors are also reported at higher levels with less detail
- Multiple error invocations may occur with detailed information
- Essential for graceful handling of JIT compilation failures without crashing PostgreSQL
- Part of PostgreSQL's LLVM JIT infrastructure error handling strategy