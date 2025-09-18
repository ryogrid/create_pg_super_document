# pg_get_client_encoding

## Location
[src/backend/utils/mb/mbutils.c:336-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L336-L344)

## Overview
Returns the numeric ID of the currently active client encoding.

## Definition
```c
int pg_get_client_encoding(void)
```

## Detailed Description
pg_get_client_encoding is a simple accessor function that returns the encoding ID of the current client encoding. It accesses the global ClientEncoding variable, which points to an entry in the pg_enc2name_tbl array containing encoding information. This function provides a clean interface for other parts of the system to query the current client encoding without directly accessing the global variable.

The returned value is a numeric encoding identifier that corresponds to the PostgreSQL internal encoding constants (e.g., PG_UTF8, PG_LATIN1, etc.).

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ClientEncoding (global variable - accesses encoding field)
- Called from:
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (in copyfrom.c:1523)
  - [BeginCopyTo](../B/BeginCopyTo.md) (in copyto.c:609)
  - [xml_send](../x/xml_send.md) (in xml.c:448)

## Notes and Other Information
- Returns the numeric encoding ID (int) of the current client encoding
- Relies on the global ClientEncoding variable being properly initialized
- Used by COPY operations and XML processing functions to determine encoding requirements
- Provides encapsulation of the ClientEncoding global variable
- The encoding ID can be converted to a name using pg_enc2name_tbl or related functions