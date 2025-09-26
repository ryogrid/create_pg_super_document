# WaitEventSet

## Location
[src/backend/storage/ipc/latch.c:102-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L102-L157)

## Overview
WaitEventSet is a core data structure in PostgreSQL that manages a collection of events that can be waited on simultaneously, providing a platform-independent interface for I/O multiplexing and event-driven programming.

## Definition

```c
struct WaitEventSet
{
	ResourceOwner owner;

	int			nevents;		/* number of registered events */
	int			nevents_space;	/* maximum number of events in this set */

	/*
	 * Array, of nevents_space length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEvent  *events;

	/*
	 * If WL_LATCH_SET is specified in any wait event, latch is a pointer to
	 * said latch, and latch_pos the offset in the ->events array. This is
	 * useful because we check the state of the latch before performing doing
	 * syscalls related to waiting.
	 */
	Latch	   *latch;
	int			latch_pos;

	/*
	 * WL_EXIT_ON_PM_DEATH is converted to WL_POSTMASTER_DEATH, but this flag
	 * is set so that we'll exit immediately if postmaster death is detected,
	 * instead of returning.
	 */
	bool		exit_on_postmaster_death;

#if defined(WAIT_USE_EPOLL)
	int			epoll_fd;
	/* epoll_wait returns events in a user provided arrays, allocate once */
	struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_KQUEUE)
	int			kqueue_fd;
	/* kevent returns events in a user provided arrays, allocate once */
	struct kevent *kqueue_ret_events;
	bool		report_postmaster_not_running;
#elif defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)

	/*
	 * Array of windows events. The first element always contains
	 * pgwin32_signal_event, so the remaining elements are offset by one (i.e.
	 * event->pos + 1).
	 */
	HANDLE	   *handles;
#endif
};
```
## Detailed Description
The WaitEventSet structure is PostgreSQL's abstraction for efficient event waiting across different operating systems. It encapsulates platform-specific I/O multiplexing mechanisms (epoll on Linux, kqueue on BSD systems, poll on other Unix systems, and Windows events on Windows) to provide a unified interface for waiting on multiple file descriptors, sockets, latches, and other events simultaneously.

This structure is fundamental to PostgreSQL's asynchronous I/O operations, enabling the server to efficiently handle multiple concurrent connections and internal events without blocking. It supports various event types including socket readiness, latch signaling, postmaster death detection, and timeouts.

The implementation uses compile-time conditionals to select the most efficient platform-specific mechanism available, ensuring optimal performance across different operating systems while maintaining a consistent API.

## Parameters / Member Variables
- `owner`: ResourceOwner that tracks this WaitEventSet for cleanup purposes
- `nevents`: Current number of registered events in the set
- `nevents_space`: Maximum number of events that can be stored (allocated capacity)
- `*events`: Array of WaitEvent structures defining the events to wait for
- `*latch`: Pointer to a latch if WL_LATCH_SET is specified in any wait event
- `latch_pos`: Position of the latch event in the events array for quick access
- `exit_on_postmaster_death`: Flag to exit immediately when postmaster death is detected
- `epoll_fd`: File descriptor for epoll instance (Linux-specific)
- `*epoll_ret_events`: Pre-allocated array for epoll_wait results (Linux-specific)
- `kqueue_fd`: File descriptor for kqueue instance (BSD-specific)
- `*kqueue_ret_events`: Pre-allocated array for kevent results (BSD-specific)
- `report_postmaster_not_running`: Flag for postmaster status reporting (BSD-specific)
- `*pollfds`: Array of pollfd structures for poll() system call (generic Unix)
- `*handles`: Array of Windows event handles (Windows-specific)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwner](../R/ResourceOwner.md)
  - [WaitEvent](WaitEvent.md)
  - [Latch](../L/Latch.md)
  - WAIT_USE_EPOLL
  - WAIT_USE_KQUEUE
  - WAIT_USE_POLL
  - WAIT_USE_WIN32

- Called from (representative examples):
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - [AddWaitEventToSet](../A/AddWaitEventToSet.md)
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md)
  - [WaitEventSetWait](WaitEventSetWait.md)
  - [WaitEventSetWaitBlock](WaitEventSetWaitBlock.md)
  - [GetNumRegisteredWaitEvents](../G/GetNumRegisteredWaitEvents.md)
  - [SysLoggerMain](../S/SysLoggerMain.md)
  - pq_putmessage_noblock

## Notes and Other Information
- The structure uses conditional compilation to include only the platform-specific members needed for the target operating system
- Memory management is integrated with PostgreSQL's ResourceOwner system for automatic cleanup
- The latch optimization allows quick checking of latch state before expensive syscalls
- Platform-specific event arrays are pre-allocated to avoid repeated memory allocation during wait operations
- The exit_on_postmaster_death feature provides a safety mechanism for child processes to terminate when the postmaster dies
- This is a fundamental building block for PostgreSQL's event-driven architecture and is used extensively throughout the codebase for asynchronous operations