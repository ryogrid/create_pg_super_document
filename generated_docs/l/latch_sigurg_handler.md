# latch_sigurg_handler

## Location
[src/backend/storage/ipc/latch.c:2282-2289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L2282-L2289)

## Overview
latch_sigurg_handler is a signal handler function that responds to SIGURG signals to wake up processes waiting on latches by writing to the self-pipe.

## Definition

```c
static void
latch_sigurg_handler(SIGNAL_ARGS)
```
## Detailed Description
latch_sigurg_handler is a static signal handler function that implements the signal-based latch notification mechanism on Unix systems. When a latch is set via SetLatch(), it sends a SIGURG signal to the target process. This signal handler receives that signal and, if the process is currently waiting, writes a byte to the self-pipe to wake up the waiting process.

The handler uses the global 'waiting' flag to determine if the process is currently blocked in a wait operation. This optimization prevents unnecessary self-pipe writes when the process is not actually waiting, reducing system call overhead.

The self-pipe technique is a classic Unix pattern for making signal handlers interact safely with blocking I/O operations. By writing to a pipe that is being monitored by the I/O multiplexing system (epoll, kqueue, etc.), the signal can reliably interrupt the blocking wait.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which expands to standard signal handler arguments (typically int sig)

## Dependencies
- Functions called/Symbols referenced:
  - [sendSelfPipeByte](../s/sendSelfPipeByte.md) (writes wake-up byte to self-pipe)
  - SIGNAL_ARGS (macro for signal handler signature)
  - waiting (global flag indicating if process is in wait state)
- Called from (representative examples):
  - LatchWaitSetLatchPos (signal handler registration)
  - [InitializeLatchSupport](../I/InitializeLatchSupport.md) (signal handler setup)

## Notes and Other Information
- Used specifically for SIGURG signal handling in the latch notification system
- Only writes to self-pipe when the process is actually waiting, optimizing performance
- Part of the Unix-specific latch implementation (Windows uses different mechanisms)
- Signal-safe implementation that only calls async-signal-safe functions
- Works in conjunction with SetLatch() which sends the SIGURG signal to target processes
- The self-pipe write ensures that blocking I/O operations (epoll_wait, etc.) are interrupted when a latch is set