# PGEventResultCopy

## Location
[src/interfaces/libpq/libpq-events.h:62-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.h#L62-L66)

## Overview
PGEventResultCopy is a structure used in PostgreSQL's libpq event system to pass information about result copying operations to registered event handlers.

## Definition
```c
typedef struct
{
    const PGresult *src;
    PGresult   *dest;
} PGEventResultCopy;
```

## Detailed Description
PGEventResultCopy is part of the libpq events API, specifically used for the PGEVT_RESULTCOPY event. This structure is passed to event handlers when a PGresult object is being copied using the PQcopyResult function. The event system allows applications and libraries to register callbacks that are invoked during various libpq operations, enabling them to perform custom actions or maintain state associated with database connections and results.

When PQcopyResult is called to duplicate a PGresult object, the libpq library triggers PGEVT_RESULTCOPY events for all registered event handlers. Each event handler receives a PGEventResultCopy structure containing pointers to both the source and destination result objects, allowing the handler to copy any custom data or state it may have associated with the original result.

## Parameters / Member Variables
- `src`: Pointer to the source PGresult object being copied (const-qualified as it should not be modified)
- `dest`: Pointer to the destination PGresult object that is being created as a copy

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  - [PQcopyResult](PQcopyResult.md) (in src/interfaces/libpq/fe-exec.c:388)

## Notes and Other Information
- This structure is part of the libpq events API defined in libpq-events.h
- It is used exclusively for PGEVT_RESULTCOPY events 
- Event handlers should not modify the source result object (src is const-qualified)
- The structure is stack-allocated and passed by pointer to event handlers
- Event handlers can use this information to copy custom instance data from the source result to the destination result
- The events system allows libraries built on top of libpq to maintain state and perform cleanup operations automatically