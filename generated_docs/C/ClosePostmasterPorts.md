# ClosePostmasterPorts

## Location
[src/backend/postmaster/postmaster.c:1957-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1957-L2033)

## Overview
Closes all the postmaster's open sockets and file descriptors that are not needed by a child process during startup.

## Definition


## Detailed Description
ClosePostmasterPorts is called during child process startup to release file descriptors that are not needed by that child process. The postmaster still retains these file descriptors open. This function systematically closes various categories of file descriptors:

1. **WaitEventSet cleanup**: Releases resources held by the postmaster's WaitEventSet using FreeWaitEventSetAfterFork()
2. **Death watch pipe**: Closes the write end of the postmaster death watch pipe to ensure proper monitoring
3. **Listen sockets**: Closes all listening sockets (except in EXEC_BACKEND mode where they are marked FD_CLOEXEC)
4. **Syslog pipe**: Conditionally closes the read side of the syslog pipe based on the am_syslogger parameter
5. **Bonjour service**: If using Bonjour, closes the connection to the mDNS daemon

The function handles platform-specific differences between Unix and Windows systems, particularly for pipe and socket handling.

## Parameters / Member Variables
- : Boolean flag indicating whether the calling process is the syslogger process. If true, the syslog pipe read end is not closed since the syslogger needs it.

## Dependencies
- Functions called/Symbols referenced:
  - [FreeWaitEventSetAfterFork](../F/FreeWaitEventSetAfterFork.md)
  - close
  - closesocket  
  - ReleaseExternalFD
  - ereport
  - elog
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md)
  - [SubPostmasterMain](../S/SubPostmasterMain.md)

## Notes and Other Information
- Critical for proper resource management in forked child processes
- Platform-specific handling for Windows vs Unix systems
- The postmaster death watch pipe closure is particularly important for proper process monitoring
- Listen sockets are marked FD_CLOEXEC in EXEC_BACKEND mode, making explicit closure unnecessary
- Bonjour support is conditionally compiled based on USE_BONJOUR
- Error handling differs between fatal errors (postmaster death pipe) and logged warnings (listen sockets)