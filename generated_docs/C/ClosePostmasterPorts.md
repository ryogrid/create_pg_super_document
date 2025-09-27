# ClosePostmasterPorts

## Location
[src/backend/postmaster/postmaster.c:1957-2033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1957-L2033)

## Overview
Closes all the postmaster's open sockets and file descriptors that are not needed by a child process during startup.

## Definition

```c
void
ClosePostmasterPorts(bool am_syslogger)
```
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
  - [ReleaseExternalFD](../R/ReleaseExternalFD.md)
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

## Simplified Source

```c
// Simplified version of ClosePostmasterPorts
void ClosePostmasterPorts(bool am_syslogger) {
    // Step 1: Clean up postmaster's WaitEventSet
    if (pm_wait_set) {
        FreeWaitEventSetAfterFork(pm_wait_set);
        pm_wait_set = NULL;
    }

    // Step 2: Close postmaster death watch pipe (Unix only)
    // This is critical - if postmaster dies, others need to know immediately
    if (close(postmaster_alive_fds[POSTMASTER_FD_OWN]) != 0) {
        // Fatal error - this must succeed
        ereport(FATAL, "could not close postmaster death monitoring pipe");
    }
    postmaster_alive_fds[POSTMASTER_FD_OWN] = -1;
    ReleaseExternalFD();

    // Step 3: Close all listen sockets (non-EXEC_BACKEND mode)
    // These sockets accept new client connections
    if (ListenSockets) {
        for (int i = 0; i < NumListenSockets; i++) {
            if (closesocket(ListenSockets[i]) != 0) {
                // Log warning but continue - not fatal
                elog(LOG, "could not close listen socket");
            }
        }
        pfree(ListenSockets);
        NumListenSockets = 0;
        ListenSockets = NULL;
    }

    // Step 4: Close syslog pipe (unless we are the syslogger)
    // Syslogger process needs to keep the pipe open to receive log messages
    if (!am_syslogger && syslogPipe[0] >= 0) {
        close(syslogPipe[0]);
        syslogPipe[0] = -1;
    }

    // Step 5: Close Bonjour service connection (if enabled)
    if (bonjour_sdref) {
        close(DNSServiceRefSockFD(bonjour_sdref));
    }
}
```

Key simplifications made:
- Removed platform-specific #ifdef blocks for clarity
- Consolidated error handling logic
- Abstracted detailed error codes and messages
- Focused on the core sequence of cleanup operations
- Added descriptive comments for each cleanup phase
- Maintained the essential logic flow and critical error handling