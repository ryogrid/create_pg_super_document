# pqinitmask

## Location
[src/backend/libpq/pqsignal.c:41-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqsignal.c#L41-L99)

## Overview
Initializes the global signal mask sets used throughout PostgreSQL for signal management during different operational phases.

## Definition

```c
void
pqinitmask(void)
```
## Detailed Description
The  function initializes three global signal mask sets that control which signals are blocked or allowed during different phases of PostgreSQL operation:

1. **BlockSig**: Contains all signals that should be blocked during normal signal blocking operations. This includes most signals that PostgreSQL expects to receive, but excludes critical signals that should never be blocked (like SIGSEGV, SIGILL, etc.).

2. **StartupBlockSig**: Similar to BlockSig but with additional exceptions for startup operations. It excludes SIGTERM, SIGQUIT, and SIGALRM to allow proper handling of shutdown and alarm signals during startup packet collection.

3. **UnBlockSig**: Initially empty, this set is used when signals should not be blocked. It may be modified by other initialization functions like InitializeLatchSupport().

The function uses a "fill then remove" approach: it first fills the blocking sets with all possible signals using , then selectively removes critical signals that should never be blocked using conditional compilation guards to handle platform differences.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - sigemptyset
  - sigfillset  
  - sigdelset
  - Various signal constants (SIGTRAP, SIGABRT, SIGILL, SIGFPE, SIGSEGV, SIGBUS, SIGSYS, SIGCONT, SIGQUIT, SIGTERM, SIGALRM)

- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (src/backend/postmaster/postmaster.c:543)
  - [InitPostmasterChild](../I/InitPostmasterChild.md) (src/backend/utils/init/miscinit.c:133)
  - [InitStandaloneProcess](../I/InitStandaloneProcess.md) (src/backend/utils/init/miscinit.c:206)

## Notes and Other Information
- This function must be called early in the PostgreSQL initialization process to ensure proper signal handling throughout the system
- The function uses conditional compilation (#ifdef) to handle signals that may not be available on all platforms
- Critical signals like SIGSEGV, SIGILL, SIGFPE, SIGABRT, and SIGTRAP are never blocked as they typically indicate serious program errors that must be handled immediately
- StartupBlockSig allows SIGTERM, SIGQUIT, and SIGALRM during startup to enable proper shutdown and timeout handling during the initial connection phase
- The UnBlockSig set may be modified by other initialization routines, particularly InitializeLatchSupport()

## Simplified Source

```c
// Simplified version of pqinitmask
void pqinitmask(void) {
    // Initialize UnBlockSig as empty set
    sigemptyset(&UnBlockSig);

    // Start with all signals blocked for both sets
    sigfillset(&BlockSig);
    sigfillset(&StartupBlockSig);

    // Remove critical signals that should never be blocked
    // These are serious error signals that must be handled immediately
    sigdelset(&BlockSig, SIGTRAP);      // Debugger breakpoint
    sigdelset(&BlockSig, SIGABRT);      // Program abort
    sigdelset(&BlockSig, SIGILL);       // Illegal instruction
    sigdelset(&BlockSig, SIGFPE);       // Floating point exception
    sigdelset(&BlockSig, SIGSEGV);      // Segmentation violation
    sigdelset(&BlockSig, SIGBUS);       // Bus error
    sigdelset(&BlockSig, SIGSYS);       // Bad system call
    sigdelset(&BlockSig, SIGCONT);      // Continue process

    // Apply same exceptions to startup set
    sigdelset(&StartupBlockSig, SIGTRAP);
    sigdelset(&StartupBlockSig, SIGABRT);
    sigdelset(&StartupBlockSig, SIGILL);
    sigdelset(&StartupBlockSig, SIGFPE);
    sigdelset(&StartupBlockSig, SIGSEGV);
    sigdelset(&StartupBlockSig, SIGBUS);
    sigdelset(&StartupBlockSig, SIGSYS);
    sigdelset(&StartupBlockSig, SIGCONT);

    // Additional startup-specific exceptions for shutdown/timeout handling
    sigdelset(&StartupBlockSig, SIGQUIT);   // Quit signal
    sigdelset(&StartupBlockSig, SIGTERM);   // Termination signal
    sigdelset(&StartupBlockSig, SIGALRM);   // Alarm signal
}
```

Key simplifications made:
- Removed conditional compilation guards for clarity (actual code needs them for portability)
- Consolidated repetitive sigdelset operations with explanatory comments
- Grouped signals by their purpose (critical errors vs startup-specific)
- Added high-level comments explaining the logic flow
- Focused on the main algorithm: empty UnBlockSig, fill both sets, then remove exceptions