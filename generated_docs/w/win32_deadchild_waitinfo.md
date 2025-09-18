# win32_deadchild_waitinfo

## Location
src/backend/postmaster/postmaster.c: 462 - 469

## Overview
win32_deadchild_waitinfo is a Windows-specific struct that holds information needed to track child process completion using Windows' I/O completion ports mechanism.

## Definition


## Detailed Description
win32_deadchild_waitinfo is a Windows-specific data structure used to implement child process monitoring in PostgreSQL's postmaster on Windows platforms. Since Windows doesn't have the same signal-based child process notification mechanisms as Unix systems, PostgreSQL uses Windows' I/O completion ports and thread pool APIs to asynchronously monitor child process termination.

This struct encapsulates the necessary Windows handles and identifiers required to track a child process and receive notification when it terminates. The structure is used in conjunction with RegisterWaitForSingleObject() and completion port mechanisms to provide Unix-like waitpid() functionality on Windows.

## Parameters / Member Variables
- : Handle returned by RegisterWaitForSingleObject(), used to unregister the wait operation when no longer needed
- : Windows process handle for the child process being monitored
- : Windows process ID (DWORD) of the child process being tracked

## Dependencies
- Functions called/Symbols referenced:
  - Windows API functions (RegisterWaitForSingleObject, UnregisterWaitEx, GetExitCodeProcess)
  - I/O completion port functions (PostQueuedCompletionStatus, GetQueuedCompletionStatus)
- Called from (representative examples):
  - waitpid (Windows implementation for child process waiting)
  - pgwin32_register_deadchild_callback (registers child process for monitoring)
  - pgwin32_deadchild_callback (callback function when child terminates)

## Notes and Other Information
- Only available on WIN32 platforms, enclosed in #ifdef WIN32 conditional compilation
- Part of PostgreSQL's cross-platform child process management abstraction
- Memory for this struct is allocated with palloc() and freed after child process completion
- Used with Windows thread pool APIs to provide asynchronous child process termination notification
- Enables PostgreSQL to maintain Unix-like process management semantics on Windows
- Critical for proper cleanup of child processes and prevention of zombie processes on Windows