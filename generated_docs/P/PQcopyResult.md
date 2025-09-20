# PQcopyResult

## Location
[src/interfaces/libpq/fe-exec.c:318-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L318-L407)

## Overview
Creates a deep copy of a PGresult with configurable copying options for attributes, tuples, events, and notice hooks, always setting the result status to PGRES_TUPLES_OK.

## Definition

```c
PGresult *
PQcopyResult(const PGresult *src, int flags)
```
## Detailed Description
PQcopyResult performs a selective deep copy of a PGresult based on the provided flags. The function creates a new empty result with PGRES_TUPLES_OK status and then conditionally copies various components from the source. The copying process is carefully ordered to handle dependencies - attributes must be copied before tuples since tuples depend on attribute metadata.

The function supports copying attributes (column metadata), tuples (data rows), events (registered event handlers), and notice hooks. When copying events, it triggers PGEVT_RESULTCOPY events for any previously initialized event handlers. The cmdStatus and client_encoding are always copied regardless of flags. Notably, error messages from the source are intentionally not copied.

## Parameters / Member Variables
- : Source PGresult to copy from (cannot be NULL)
- : Bitwise OR of copy options:
  - : Copy column attributes/metadata
  - : Copy data tuples (implies PG_COPYRES_ATTRS)
  - : Copy event handlers
  - : Copy notice callback hooks

## Dependencies
- Functions called/Symbols referenced:
  - [PQmakeEmptyPGresult](PQmakeEmptyPGresult.md)
  - [PQsetResultAttrs](PQsetResultAttrs.md)
  - [PQsetvalue](PQsetvalue.md)
  - [dupEvents](../d/dupEvents.md)
  - [PQclear](PQclear.md)
- Called from (representative examples):
  - [pqRowProcessor](../p/pqRowProcessor.md)

## Notes and Other Information
- Returns NULL if source is NULL or if any copying operation fails
- Always creates result with PGRES_TUPLES_OK status regardless of source status
- Error messages are intentionally not copied from source
- PG_COPYRES_TUPLES implies PG_COPYRES_ATTRS since tuples require attribute metadata
- Event handlers receive PGEVT_RESULTCOPY notifications when copied
- Memory management ensures proper cleanup on any failure during copying process
- Cannot be used with PQsetResultAttrs if PG_COPYRES_ATTRS or PG_COPYRES_TUPLES flags are used