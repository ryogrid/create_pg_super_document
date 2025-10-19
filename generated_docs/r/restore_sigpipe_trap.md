# restore_sigpipe_trap

## Location
[src/fe_utils/print.c:3062-3074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3062-L3074)

## Overview
A utility function that restores normal SIGPIPE signal handling after temporary modifications, with intelligent behavior that depends on the context and permanent output configuration.

## Definition
```c
void restore_sigpipe_trap(void)
```

## Detailed Description
This function restores SIGPIPE signal handling to its appropriate state after temporary pipe operations are complete. The behavior is context-aware:

1. **In psql**: Restores SIGPIPE handling based on the `always_ignore_sigpipe` flag, which is typically set when the permanent query output file is a pipe (like when output is redirected to a pager or file)
2. **In other programs**: Always restores default SIGPIPE handling (SIG_DFL)

The function works in tandem with disable_sigpipe_trap() to provide safe pipe operations. When `always_ignore_sigpipe` is true (usually because the main output is piped), SIGPIPE remains ignored (SIG_IGN) to prevent termination when writing to broken pipes. Otherwise, it restores default behavior (SIG_DFL) where SIGPIPE would terminate the process.

This approach is designed for psql's current architecture where nested pipe operations are not complex enough to require a full save/restore state mechanism.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md) (PostgreSQL's signal handling wrapper)
  - SIGPIPE (signal constant)
  - SIG_IGN (ignore signal handler)
  - SIG_DFL (default signal handler)
  - always_ignore_sigpipe (global flag determining signal behavior)
- Called from (representative examples):
  - [exec_command_write](../e/exec_command_write.md) (at src/bin/psql/command.c:2838)
  - [do_watch](../d/do_watch.md) (at src/bin/psql/command.c:5414, 5542)
  - [CloseGOutput](../C/CloseGOutput.md) (at src/bin/psql/common.c:117)
  - [setQFout](../s/setQFout.md) (at src/bin/psql/common.c:155)
  - [do_copy](../d/do_copy.md) (at src/bin/psql/copy.c:395)
  - [PageOutput](../P/PageOutput.md) (at src/fe_utils/print.c:3128)
  - [ClosePager](../C/ClosePager.md) (at src/fe_utils/print.c:3157)

## Notes and Other Information
- This is a public function (not static), accessible from other modules
- Platform-specific: Only functional on Unix-like systems, no-op on Windows
- Uses pqsignal instead of standard signal() for PostgreSQL's signal handling consistency
- The `always_ignore_sigpipe` flag is typically set based on whether the permanent output destination is a pipe
- Designed to work with disable_sigpipe_trap() as a pair for safe pipe operations
- The current implementation is optimized for psql's relatively simple pipe usage patterns
- Future complex nested pipe operations might require a more sophisticated save/restore mechanism
- Critical for proper cleanup in PostgreSQL frontend tools after pipe operations
- Part of PostgreSQL's frontend utilities for robust I/O handling

## Simplified Source

```c
void
restore_sigpipe_trap(void)
{
#ifndef WIN32
    // Restore SIGPIPE handling based on context:
    // - If always_ignore_sigpipe is true (main output is piped), keep ignoring
    // - Otherwise, restore default behavior (process termination on SIGPIPE)
    pqsignal(SIGPIPE, always_ignore_sigpipe ? SIG_IGN : SIG_DFL);
#endif
    // No-op on Windows - SIGPIPE doesn't exist there
}
```