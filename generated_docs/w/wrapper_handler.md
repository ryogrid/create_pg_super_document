# wrapper_handler

## Location
src/port/pqsignal.c: 86 - 134

## Overview
A signal wrapper handler function that acts as an intermediate handler for all signals set up by pqsignal(), ensuring proper signal handling within PostgreSQL processes while protecting against modifications from child processes.

## Definition
```c
static void wrapper_handler(SIGNAL_ARGS)
```

## Detailed Description
The wrapper_handler function serves as a protective wrapper around user-provided signal handlers in PostgreSQL. When pqsignal() is called with a signal handler (not SIG_IGN or SIG_DFL), it actually registers wrapper_handler as the system-level signal handler, while storing the users original handler in the pqsignal_handlers array.