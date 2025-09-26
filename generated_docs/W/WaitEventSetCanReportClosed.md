# WaitEventSetCanReportClosed

## Location
src/backend/storage/ipc/latch.c: 2254 - 2268

## Overview
WaitEventSetCanReportClosed is a capability detection function that returns whether the current platform and build configuration supports reporting WL_SOCKET_CLOSED events.

## Definition

```c
bool
WaitEventSetCanReportClosed(void)
```
## Detailed Description
WaitEventSetCanReportClosed is a simple compile-time capability detection function that determines whether the current PostgreSQL build can report socket closure events (WL_SOCKET_CLOSED). The function checks for the availability of platform-specific primitives that can detect socket closure: poll() with POLLRDHUP support, epoll, or kqueue.

This function allows PostgreSQL code to conditionally enable features that depend on socket closure detection. Without this capability, the system cannot reliably detect when a client has closed its connection, which may impact connection management and cleanup behavior.

The function uses preprocessor conditionals to check for the following conditions:
- WAIT_USE_POLL is defined AND POLLRDHUP is available (Linux poll with hangup detection)
- WAIT_USE_EPOLL is defined (Linux epoll interface)
- WAIT_USE_KQUEUE is defined (BSD/macOS kqueue interface)

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - WAIT_USE_POLL (compile-time macro)
  - WAIT_USE_EPOLL (compile-time macro)
  - WAIT_USE_KQUEUE (compile-time macro)
  - POLLRDHUP (compile-time macro, when available)
- Called from (representative examples):
  - check_client_connection_check_interval

## Notes and Other Information
- Returns true if WL_SOCKET_CLOSED events can be reported, false otherwise
- This is a pure compile-time determination based on available platform primitives
- Used by connection management code to determine if client disconnect detection is available
- Essential for enabling features like client_connection_check_interval which rely on socket closure detection
- The function helps maintain portability across different Unix-like systems with varying I/O multiplexing capabilities