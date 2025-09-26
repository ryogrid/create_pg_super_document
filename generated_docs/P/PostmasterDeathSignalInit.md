# PostmasterDeathSignalInit

## Location
[src/backend/storage/ipc/pmsignal.c:437-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L437-L462)

## Overview
Initializes signal-based notification mechanism to detect when the postmaster process dies, if supported by the platform.

## Definition

```c
void
PostmasterDeathSignalInit(void)
```
## Detailed Description
This function sets up a fast notification mechanism for detecting postmaster death on platforms that support parent death signaling. It registers a signal handler and configures the operating system to send a signal to the current process when its parent (the postmaster) terminates.

The function works in two steps:
1. Registers postmaster_death_handler() as the signal handler for POSTMASTER_DEATH_SIGNAL
2. Uses platform-specific system calls to request parent death notification:
   - On Linux: Uses prctl(PR_SET_PDEATHSIG) 
   - On FreeBSD: Uses procctl(PROC_PDEATHSIG_CTL)

The mechanism provides a fast path for postmaster death detection by setting the postmaster_possibly_dead flag when the signal is received. This avoids the need for expensive system calls in the common case where the postmaster is still alive.

As a safety measure, the function sets postmaster_possibly_dead to true initially, ensuring that the first call to PostmasterIsAlive() will use the slow path to verify the postmaster's status.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : Global flag set to true initially and by the signal handler
- Platform-specific signal number (SIGINFO or SIGPWR depending on availability)

## Dependencies
- Signal handling:
  - pqsignal() - PostgreSQL signal registration function
  - postmaster_death_handler() - Signal handler function
- Platform-specific system calls:
  - prctl(PR_SET_PDEATHSIG) (Linux)
  - procctl(PROC_PDEATHSIG_CTL) (FreeBSD) 
- Constants used:
  - POSTMASTER_DEATH_SIGNAL (SIGINFO or SIGPWR)
  - PR_SET_PDEATHSIG (Linux)
  - PROC_PDEATHSIG_CTL (FreeBSD)
- Conditional compilation:
  - USE_POSTMASTER_DEATH_SIGNAL
- Called from:
  - InitPostmasterChild (src/backend/utils/init/miscinit.c:165)

## Notes and Other Information
- Only compiled and functional on platforms that support USE_POSTMASTER_DEATH_SIGNAL
- The signal handler simply sets postmaster_possibly_dead = true for later checking
- Provides significant performance benefit by avoiding frequent system calls to check postmaster status
- Falls back gracefully on platforms without parent death signaling support
- Critical for proper cleanup and error handling in child processes when postmaster terminates