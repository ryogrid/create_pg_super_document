# PQregisterEventProc

## Location
[src/interfaces/libpq/libpq-events.c:40-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.c#L40-L97)

## Overview
Registers an event procedure with a PostgreSQL connection object to receive notifications about connection-related events.

## Definition
```c
int PQregisterEventProc(PGconn *conn, PGEventProc proc, const char *name, void *passThrough)
```

## Detailed Description
PQregisterEventProc allows applications to register callback functions that will be invoked when specific events occur on a PostgreSQL connection. The event system provides a mechanism for libraries and applications to hook into libpq operations and perform custom actions. Each event procedure can be registered only once per connection, using the procedure address as a unique identifier. When registered, the procedure is immediately called with a PGEVT_REGISTER event to allow initialization. The function manages dynamic allocation of the events array, expanding it as needed to accommodate new registrations.

## Parameters / Member Variables
- `conn`: The PGconn connection object to register the event procedure with
- `proc`: The event procedure function pointer that will be called for events  
- `name`: A descriptive name for the event procedure used in error messages (copied internally)
- `passThrough`: Application-specific data pointer passed to the event procedure on each call

## Dependencies
- Functions called/Symbols referenced:
  - [PGEventRegister](PGEventRegister.md) (event data structure)
  - [PGEvent](PGEvent.md) (event array element type)
  - realloc/malloc (for dynamic array management)
  - PGEVT_REGISTER (registration event type)
- Called from (representative examples):
  - PGEventResultDestroy (libpq-events.h:72)

## Notes and Other Information
The same procedure cannot be registered multiple times on the same connection. The name parameter must be non-empty and is copied internally. The function returns true on success, false on failure. Memory allocation failures or duplicate registrations will cause failure. The passThrough pointer is stored and passed to the event procedure unchanged.

## Simplified Source

```c
int PQregisterEventProc(PGconn *conn, PGEventProc proc, const char *name, void *passThrough) {
    int i;
    PGEventRegister regevt;

    // Validate required parameters
    if (!proc || !conn || !name || !*name)
        return false;

    // Check if procedure is already registered
    for (i = 0; i < conn->nEvents; i++) {
        if (conn->events[i].proc == proc)
            return false;
    }

    // Expand events array if needed
    if (conn->nEvents >= conn->eventArraySize) {
        PGEvent *e;
        int newSize = conn->eventArraySize ? conn->eventArraySize * 2 : 8;

        e = conn->events ? realloc(conn->events, newSize * sizeof(PGEvent))
                        : malloc(newSize * sizeof(PGEvent));
        if (!e)
            return false;

        conn->eventArraySize = newSize;
        conn->events = e;
    }

    // Add new event entry
    conn->events[conn->nEvents].proc = proc;
    conn->events[conn->nEvents].name = strdup(name);
    if (!conn->events[conn->nEvents].name)
        return false;
    conn->events[conn->nEvents].passThrough = passThrough;
    conn->events[conn->nEvents].data = NULL;
    conn->events[conn->nEvents].resultInitialized = false;
    conn->nEvents++;

    // Call registration event
    regevt.conn = conn;
    if (!proc(PGEVT_REGISTER, &regevt, passThrough)) {
        // Rollback on failure
        conn->nEvents--;
        free(conn->events[conn->nEvents].name);
        return false;
    }

    return true;
}
```