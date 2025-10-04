# libpqrcv_connect

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:143-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L143-L315)

## Overview
Establishes a connection to a PostgreSQL primary server for WAL streaming replication, supporting both logical and physical replication modes.

## Definition
```c
static WalReceiverConn *libpqrcv_connect(const char *conninfo, bool replication, bool logical,
                                       bool must_use_password, const char *appname, char **err)
```

## Detailed Description
The `libpqrcv_connect` function establishes a PostgreSQL connection specifically for WAL (Write-Ahead Log) streaming replication purposes. It can handle both regular database connections and replication connections (logical or physical). The function implements asynchronous connection establishment using libpq's polling mechanism to avoid blocking the server process.

For logical replication, the function configures specific client settings including encoding translation and GUC parameters to ensure consistent data interpretation between publisher and subscriber. For physical replication, it sets up a replication-mode connection.

The function includes robust error handling and security measures, including password validation for non-superusers and secure search path configuration to prevent malicious SQL injection.

## Parameters / Member Variables
- `conninfo`: Connection string or URI specifying the target PostgreSQL server
- `replication`: Whether this is a replication connection (true) or regular connection (false)
- `logical`: For replication connections, whether it's logical (true) or physical (false) replication
- `must_use_password`: If true, connection must use password authentication (security requirement)
- `appname`: Application name to identify the connection in server logs and statistics
- `err`: Output parameter for error messages (palloc'ed string on failure)

## Dependencies
- Functions called/Symbols referenced:
  - [libpqrcv_check_conninfo](libpqrcv_check_conninfo.md) (validate connection parameters)
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md) (for logical replication encoding)
  - [PQconnectStartParams](../P/PQconnectStartParams.md) (initiate asynchronous connection)
  - [PQconnectPoll](../P/PQconnectPoll.md) (advance connection state machine)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (wait for socket events)
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md) (handle interrupts during connection)
  - [libpqrcv_PQexec](libpqrcv_PQexec.md) (execute secure search path SQL)
  - Various libpq functions (PQstatus, PQconnectionUsedPassword, etc.)

- Called from (representative examples):
  - Registered in `PQWalReceiverFunctions` table as `walrcv_connect`
  - Used by WAL receiver processes for establishing replication connections
  - Invoked by logical replication workers

## Notes and Other Information
- Uses asynchronous connection establishment to prevent blocking the server process
- Implements special handling for logical replication including encoding and GUC parameter setup
- Enforces password authentication requirements for security when `must_use_password` is true
- Sets secure search path for connections that will execute SQL queries
- Connection polling respects process interrupts and latch signals
- Returns NULL on failure with error message in `err` parameter
- May call ereport(ERROR) for password-related security violations instead of returning NULL
- Supports both database connections and replication protocol connections

## Simplified Source

```c
static WalReceiverConn *
libpqrcv_connect(const char *conninfo, bool replication, bool logical,
                 bool must_use_password, const char *appname, char **err)
{
    WalReceiverConn *conn;
    PostgresPollingStatusType status;
    const char *keys[6];
    const char *vals[6];
    int i = 0;

    // Re-validate connection string with current security context
    libpqrcv_check_conninfo(conninfo, must_use_password);

    // Build connection parameters
    keys[i] = "dbname";
    vals[i] = conninfo;

    Assert(replication || !logical);

    if (replication)
    {
        keys[++i] = "replication";
        vals[i] = logical ? "database" : "true";

        if (logical)
        {
            // Set encoding and GUC parameters for logical replication
            keys[++i] = "client_encoding";
            vals[i] = GetDatabaseEncodingName();
            keys[++i] = "options";
            vals[i] = "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3";
        }
        else
        {
            // Physical replication uses "replication" database name
            keys[++i] = "dbname";
            vals[i] = "replication";
        }
    }

    keys[++i] = "fallback_application_name";
    vals[i] = appname;
    keys[++i] = NULL;
    vals[i] = NULL;

    // Start asynchronous connection
    conn = palloc0(sizeof(WalReceiverConn));
    conn->streamConn = PQconnectStartParams(keys, vals, true);
    if (PQstatus(conn->streamConn) == CONNECTION_BAD)
        goto bad_connection_errmsg;

    // Poll until connection completes
    status = PGRES_POLLING_WRITING;
    do
    {
        int io_flag = (status == PGRES_POLLING_READING) ? WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE;

#ifdef WIN32
        if (PQstatus(conn->streamConn) == CONNECTION_STARTED)
            io_flag = WL_SOCKET_CONNECTED;
#endif

        int rc = WaitLatchOrSocket(MyLatch, WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | io_flag,
                                   PQsocket(conn->streamConn), 0, WAIT_EVENT_LIBPQWALRECEIVER_CONNECT);

        // Handle interrupts
        if (rc & WL_LATCH_SET)
        {
            ResetLatch(MyLatch);
            ProcessWalRcvInterrupts();
        }

        // Advance connection state machine
        if (rc & io_flag)
            status = PQconnectPoll(conn->streamConn);
    } while (status != PGRES_POLLING_OK && status != PGRES_POLLING_FAILED);

    if (PQstatus(conn->streamConn) != CONNECTION_OK)
        goto bad_connection_errmsg;

    // Enforce password requirement for security
    if (must_use_password && !PQconnectionUsedPassword(conn->streamConn))
    {
        PQfinish(conn->streamConn);
        pfree(conn);
        ereport(ERROR, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                       errmsg("password is required"),
                       errdetail("Non-superuser cannot connect if the server does not request a password.")));
    }

    // Set secure search path for SQL query execution
    if (!replication || logical)
    {
        PGresult *res = libpqrcv_PQexec(conn->streamConn, ALWAYS_SECURE_SEARCH_PATH_SQL);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            PQclear(res);
            *err = psprintf(_("could not clear search path: %s"), pchomp(PQerrorMessage(conn->streamConn)));
            goto bad_connection;
        }
        PQclear(res);
    }

    conn->logical = logical;
    return conn;

bad_connection_errmsg:
    *err = pchomp(PQerrorMessage(conn->streamConn));
bad_connection:
    PQfinish(conn->streamConn);
    pfree(conn);
    return NULL;
}
```