# SyncVariables

## Location
[src/bin/psql/command.c:4040-4082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4040-L4082)

## Overview
Synchronizes psql's internal variables with the current database connection state, ensuring consistency between the client configuration and the established connection parameters.

## Definition
```c
void SyncVariables(void)
```

## Detailed Description
This function is called whenever a new database connection is established to update psql's internal state variables to match the connection properties. It performs a bidirectional synchronization: first retrieving connection information from the PostgreSQL server and updating internal variables, then sending client-side settings back to the server.

The function updates both the global pset structure and psql's variable system with current connection details including database name, user, host, port, encoding, and server version information. It also ensures that client-side error reporting preferences are applied to the connection.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [PQclientEncoding](../P/PQclientEncoding.md)()
  - [PQserverVersion](../P/PQserverVersion.md)()
  - [setFmtEncoding](../s/setFmtEncoding.md)()
  - [PQdb](../P/PQdb.md)()
  - [SetVariable](SetVariable.md)() (multiple calls)
  - [PQuser](../P/PQuser.md)()
  - [PQhost](../P/PQhost.md)()
  - [PQport](../P/PQport.md)()
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md)()
  - [PQparameterStatus](../P/PQparameterStatus.md)()
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)()
  - [PQsetErrorVerbosity](../P/PQsetErrorVerbosity.md)()
  - [PQsetErrorContextVisibility](../P/PQsetErrorContextVisibility.md)()
- Called from:
  - [do_connect](../d/do_connect.md) (at src/bin/psql/command.c:3794)
  - [CheckConnection](../C/CheckConnection.md) (at src/bin/psql/common.c:383)
  - Various locations in startup.c

## Notes and Other Information
- Updates the following psql variables: DBNAME, USER, HOST, PORT, ENCODING, SERVER_VERSION_NAME, SERVER_VERSION_NUM
- Synchronizes both pset.encoding and pset.popt.topt.encoding with the connection's client encoding
- Attempts to get the full server version string first via PQparameterStatus(), falling back to formatting the numeric version if unavailable
- Applies client-side error verbosity and context visibility settings to the connection
- Critical for maintaining consistency when switching between different database connections in psql

## Simplified Source

```c
void SyncVariables(void) {
    char vbuf[32];
    const char *server_version;

    // Get connection encoding and version info
    pset.encoding = PQclientEncoding(pset.db);
    pset.popt.topt.encoding = pset.encoding;
    pset.sversion = PQserverVersion(pset.db);
    setFmtEncoding(pset.encoding);

    // Set connection-related psql variables
    SetVariable(pset.vars, "DBNAME", PQdb(pset.db));
    SetVariable(pset.vars, "USER", PQuser(pset.db));
    SetVariable(pset.vars, "HOST", PQhost(pset.db));
    SetVariable(pset.vars, "PORT", PQport(pset.db));
    SetVariable(pset.vars, "ENCODING", pg_encoding_to_char(pset.encoding));

    // Get server version string (prefer full text, fallback to numeric)
    server_version = PQparameterStatus(pset.db, "server_version");
    if (!server_version) {
        formatPGVersionNumber(pset.sversion, true, vbuf, sizeof(vbuf));
        server_version = vbuf;
    }
    SetVariable(pset.vars, "SERVER_VERSION_NAME", server_version);

    snprintf(vbuf, sizeof(vbuf), "%d", pset.sversion);
    SetVariable(pset.vars, "SERVER_VERSION_NUM", vbuf);

    // Apply client error reporting settings to connection
    PQsetErrorVerbosity(pset.db, pset.verbosity);
    PQsetErrorContextVisibility(pset.db, pset.show_context);
}
```