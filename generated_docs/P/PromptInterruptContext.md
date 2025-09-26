# PromptInterruptContext

## Location
[src/include/common/string.h:17-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/string.h#L17-L23)

## Overview
PromptInterruptContext is a structure that provides a mechanism for safely canceling interactive prompts through signal handling, specifically SIGINT interrupts, using longjmp-based control flow.

## Definition

```c
typedef struct PromptInterruptContext
{
	/* To avoid including <setjmp.h> here, jmpbuf is declared "void *" */
	void	   *jmpbuf;			/* existing longjmp buffer */
	volatile sig_atomic_t *enabled; /* flag that enables longjmp-on-interrupt */
	bool		canceled;		/* indicates whether cancellation occurred */
} PromptInterruptContext;
```
## Detailed Description
The PromptInterruptContext structure is designed to provide safe, interruptible user input functionality in PostgreSQL's client tools, particularly psql. It implements a cooperative cancellation mechanism that allows long-running or blocking I/O operations (like reading user input) to be interrupted by signals without leaving the system in an inconsistent state.

The structure works in conjunction with signal handlers that can perform longjmp operations to break out of potentially blocking operations like fgets() or console I/O. This is particularly important for interactive database clients where users expect to be able to cancel operations using Ctrl+C.

The design uses a three-part coordination mechanism:
1. A longjmp buffer (jmpbuf) that defines the target location to jump to when cancellation occurs
2. An enabled flag that controls whether the signal handler should actually perform the longjmp
3. A canceled flag that indicates whether cancellation actually occurred

This approach ensures that cancellation only happens when it's safe to do so, and provides feedback about whether the operation completed normally or was canceled.

## Parameters / Member Variables
- `*jmpbuf`: A pointer to a setjmp/longjmp buffer that serves as the target for cancellation jumps. Declared as void* to avoid including setjmp.h in the header file.
- `*enabled`: A pointer to a volatile sig_atomic_t flag that controls whether signal handlers should perform longjmp operations. The volatile and sig_atomic_t qualifiers ensure safe access from signal handlers.
- `canceled`: A boolean flag that indicates whether the operation was actually canceled via signal interruption, allowing callers to distinguish between normal completion and cancellation.
## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
  - Uses setjmp/longjmp mechanism (indirectly)
  - Relies on signal handling infrastructure

- Called from (representative examples):
  - [simple_prompt_extended](../s/simple_prompt_extended.md) (src/common/sprompt.c:54)
  - [pg_get_line](../p/pg_get_line.md) (src/common/pg_get_line.c:59)
  - [pg_get_line_append](../p/pg_get_line_append.md) (src/common/pg_get_line.c:125)
  - [exec_command_password](../e/exec_command_password.md) (src/bin/psql/command.c:2136)
  - [exec_command_prompt](../e/exec_command_prompt.md) (src/bin/psql/command.c:2224)
  - [prompt_for_password](../p/prompt_for_password.md) (src/bin/psql/command.c:3338)

## Notes and Other Information
- The structure is defined in src/include/common/string.h, making it available to both frontend and backend code
- The jmpbuf member is intentionally declared as void* rather than jmp_buf to avoid requiring setjmp.h inclusion in the header
- The enabled flag uses sig_atomic_t to ensure atomic access from signal handlers, following POSIX signal safety requirements
- This mechanism is primarily used in interactive contexts where user cancellation is expected and desired
- The design allows for nested or layered cancellation contexts by using different PromptInterruptContext instances
- Care must be taken to ensure that the enabled flag and jmpbuf remain valid for the duration of the interruptible operation
- The canceled flag provides important feedback to calling code about whether normal completion occurred or the operation was interrupted