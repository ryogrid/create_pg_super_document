# llvm_shutdown

## Location
[src/backend/jit/llvm/llvmjit.c:993-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L993-L1051)

## Overview
Cleans up LLVM JIT resources during PostgreSQL process shutdown, disposing of ORC JIT instances and thread-safe contexts while handling fatal error scenarios.

## Definition

```c
static void
llvm_shutdown(int code, Datum arg)
```
## Detailed Description
This function serves as the cleanup handler for LLVM JIT resources, registered via  during session initialization. It performs careful resource cleanup while handling edge cases:

**Safety Checks:**
- Detects if shutdown occurs during a fatal-on-oom section and safely returns without cleanup to avoid calling back into potentially corrupted LLVM state
- Verifies that no JIT contexts are still in use, triggering a PANIC if any remain active

**Resource Cleanup (Version-Dependent):**
- **LLVM > 11**: Disposes of LLJIT instances and ThreadSafeContext using the new ORC API
- **LLVM ≤ 11**: Disposes of legacy ORC instances, including flushing profiling data

The function ensures all LLVM resources are properly released to prevent memory leaks and allow profiling data to be written out.

## Parameters / Member Variables
- `code`: Exit code (standard on_proc_exit parameter, unused)
- `arg`: Datum argument (standard on_proc_exit parameter, unused)
## Dependencies
- Functions called/Symbols referenced:
  - PANIC (error level for assertion failures)
  - llvm_in_fatal_on_oom (safety check function - implicitly referenced)
- Called from (representative examples):
  - [llvm_session_initialize](llvm_session_initialize.md) (via on_proc_exit registration)

## Notes and Other Information
- Registered as a process exit callback during llvm_session_initialize
- Includes safety mechanism to avoid cleanup during fatal-on-oom conditions where LLVM state may be corrupted
- Performs sanity check to ensure all JIT contexts have been properly released before shutdown
- Handles LLVM version differences gracefully with conditional compilation
- For LLVM ≤ 11, ensures profiling data is properly flushed during cleanup
- Sets global ORC instance pointers to NULL after disposal to prevent use-after-free
- Critical for preventing resource leaks in long-running PostgreSQL processes
- The function signature matches the on_proc_exit callback requirements (int, Datum)