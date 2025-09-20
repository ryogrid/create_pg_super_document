# atexit_callback

## Location
[src/backend/storage/ipc/ipc.c:294-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L294-L308)

## Overview
The atexit_callback function serves as an emergency backstop to ensure proper cleanup when PostgreSQL processes are terminated through direct calls to exit() rather than the preferred proc_exit() function.

## Definition

```c
static void
atexit_callback(void)
```
## Detailed Description
atexit_callback implements a safety mechanism for handling improper process termination in PostgreSQL. It is registered with the system's atexit() facility and automatically invoked when a process exits through direct calls to exit() rather than the proper proc_exit() pathway. The function calls proc_exit_prepare() with an exit code of -1 (since the real exit code is not available at this point) to ensure that essential cleanup operations are still performed. This design provides a fallback cleanup mechanism for add-on code or other components that might not follow PostgreSQL's preferred termination protocol.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [proc_exit_prepare](../p/proc_exit_prepare.md)
- Called from (representative examples):
  - Registered as atexit handler in on_proc_exit
  - Registered as atexit handler in before_shmem_exit  
  - Registered as atexit handler in on_shmem_exit

## Notes and Other Information
- Declared as static, only accessible within the same source file
- Uses exit code -1 since real exit code is unavailable in atexit context
- Serves as backstop against uncooperative add-on code calling exit() directly
- Does not protect against _exit() calls, which bypass atexit handlers entirely
- _exit() protection is handled by "dead man switch" mechanism in pmsignal.c
- Automatically registered when callback registration functions are first used
- Ensures cleanup happens even when proper termination protocol is not followed