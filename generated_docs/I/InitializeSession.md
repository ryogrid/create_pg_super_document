# InitializeSession

## Location
[src/backend/access/common/session.c:54-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/session.c#L54-L69)

## Overview
Initializes the current backend's session by allocating memory for an empty Session object and setting up the global CurrentSession pointer.

## Definition

```c
void
InitializeSession(void)
```
## Detailed Description
InitializeSession creates a new Session object in the TopMemoryContext and assigns it to the global CurrentSession variable. This function sets up the basic session infrastructure that enables sharing of state between backends performing work for a client session, particularly in parallel query execution scenarios. The allocated Session object is initially empty (zero-initialized) and will be populated by subsequent operations like GetSessionDsmHandle() when shared memory segments are needed.

The function uses MemoryContextAllocZero to ensure all fields in the Session struct are initialized to NULL/0, providing a clean starting state for the session infrastructure.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [Session](../S/Session.md) (struct type)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md) (in src/backend/utils/init/postinit.c:1236)

## Notes and Other Information
- The Session object is allocated in TopMemoryContext to ensure it persists for the lifetime of the backend process
- This function only creates the basic Session structure; actual DSM segments and shared areas are created later by GetSessionDsmHandle() when needed
- The Session infrastructure is primarily used for parallel query execution to share state like typemod registries between leader and worker processes
- Must be called before any other session-related operations that depend on CurrentSession being non-NULL