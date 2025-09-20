# sigaction

## Location
[src/include/libpq/pqsignal.h:25-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqsignal.h#L25-L36)

## Overview
sigaction is a Windows-specific structure that emulates the POSIX sigaction structure for signal handling in PostgreSQL's Windows port.

## Definition

```c
struct sigaction
{
	void		(*sa_handler) (int);
	/* sa_sigaction not yet implemented */
	sigset_t	sa_mask;
	int			sa_flags;
};
```
## Detailed Description
The sigaction structure is defined in PostgreSQL's Windows compatibility layer to provide POSIX-like signal handling functionality on Windows platforms. This structure is used in conjunction with the pqsigaction function to install and examine signal handlers. It serves as a bridge between PostgreSQL's cross-platform signal handling requirements and Windows' native signal mechanisms.

The structure is part of PostgreSQL's effort to maintain consistent signal handling behavior across different operating systems, allowing the same high-level signal handling code to work on both Unix-like systems and Windows.

## Parameters / Member Variables
- : Function pointer to the signal handler function that takes an integer signal number as parameter
- : Signal set that specifies signals to be blocked during execution of the signal handler
- : Flags that modify the behavior of the signal handler (supports SA_RESTART and SA_NODEFER)

## Dependencies
- Functions called/Symbols referenced:
  - sigset_t
- Called from (representative examples):
  - [pqsigaction](../p/pqsigaction.md)
  - [pgwin32_dispatch_queued_signals](../p/pgwin32_dispatch_queued_signals.md)
  - [pqsignal](../p/pqsignal.md)

## Notes and Other Information
- This structure is only defined when compiling on Windows (within #ifdef WIN32 blocks)
- The sa_sigaction member is noted as "not yet implemented", indicating that only basic signal handler functionality is supported
- Works in conjunction with pqsigaction function to provide POSIX sigaction compatibility
- Part of the broader Windows signal emulation system that includes pqsigprocmask and related signal handling functions
- The actual sigaction macro on line 41 redirects calls to pqsigaction for Windows compatibility