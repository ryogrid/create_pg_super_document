# connectOptions1

## Location
[src/interfaces/libpq/fe-connect.c:997-1033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L997-L1033)

## Overview
Internal subroutine that parses a connection string and populates a PGconn structure with the parsed connection parameters as the first stage of connection setup.

## Definition
```c
static bool connectOptions1(PGconn *conn, const char *conninfo)
```

## Detailed Description
This function handles the first phase of connection option processing for libpq. It takes a connection info string, parses it into individual connection options, and transfers those options into the PGconn structure. The function is designed to work in conjunction with pqConnectOptions2, which handles derived settings. The split allows PQsetdbLogin to override defaults between the two phases. The function sets the connection status to CONNECTION_BAD and updates the error message if any step fails.

## Parameters / Member Variables
- `conn`: PGconn structure to be populated with connection parameters
- `conninfo`: Connection string containing key=value pairs of connection parameters

## Dependencies
- Functions called/Symbols referenced:
  - [parse_connection_string](../p/parse_connection_string.md) (parses the connection string into options)
  - [fillPGconn](../f/fillPGconn.md) (transfers options into the PGconn structure)
  - [PQconninfoFree](../P/PQconninfoFree.md) (frees the connection options structure)
  - [PQconninfoOption](../P/PQconninfoOption.md) (structure type for connection options)
  - CONNECTION_BAD (connection status constant)
- Called from (representative examples):
  - [PQconnectStart](../P/PQconnectStart.md)
  - [PQsetdbLogin](../P/PQsetdbLogin.md)

## Notes and Other Information
- Returns true if successful, false on failure
- On failure, sets conn->status to CONNECTION_BAD and populates conn->errorMessage
- This is the first of a two-phase connection setup process (connectOptions1 followed by pqConnectOptions2)
- The function properly manages memory by freeing the temporary connOptions structure
- Designed to allow PQsetdbLogin to override defaults between the two connection setup phases
- Location: src/interfaces/libpq/fe-connect.c:997-1033