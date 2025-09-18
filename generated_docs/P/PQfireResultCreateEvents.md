# PQfireResultCreateEvents

## Location
[src/interfaces/libpq/libpq-events.c:185-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-events.c#L185-L211)

## Overview
Fires RESULTCREATE events for an application-created PGresult, allowing registered event procedures to initialize result-specific data.

## Definition


## Detailed Description
This function iterates through all registered event procedures associated with a PGresult and fires PGEVT_RESULTCREATE events for those that haven't been initialized yet. It's specifically designed for application-created PGresult objects that need to trigger the same event handling as server-generated results. The function ensures that each event procedure is only fired once per result by checking the  flag.

The function handles the case where multiple event procedures may be registered with a result, calling each one with a PGEventResultCreate structure containing the connection and result pointers. If any event procedure fails (returns false), the overall operation is marked as failed but other procedures are still called.

## Parameters / Member Variables
- : PGconn pointer that can be NULL if event procedures won't use the connection information
- : PGresult pointer for which to fire the RESULTCREATE events; function returns false if NULL

## Dependencies
- Functions called/Symbols referenced:
  - [PGEventResultCreate](PGEventResultCreate.md) (structure used for event data)
  - PGEVT_RESULTCREATE (event type constant)
- Called from (representative examples):
  - [PQgetResult](PQgetResult.md) (src/interfaces/libpq/fe-exec.c:2214)

## Notes and Other Information
- The function returns true if all event procedures succeed, false if any fail or if res is NULL
- Events are only fired for procedures where  is false, preventing duplicate initialization
- The conn parameter being NULL is explicitly supported for cases where event procedures don't need connection context
- This is part of PostgreSQL's libpq event system that allows applications to register callbacks for various connection and result lifecycle events
- Located in src/interfaces/libpq/libpq-events.c:185-211