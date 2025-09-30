# pqMakeEmptyPGconn

## Location
[src/interfaces/libpq/fe-connect.c:4535-4633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4535-L4633)

## Overview
Creates and initializes an empty PGconn data structure with default values, serving as the foundation for PostgreSQL client connections.

## Definition

```c
structure
 *
 * NOTE: this should not overlap any functionality with pqClosePGconn().
 * Clearing/resetting of transient state belongs there;
```
## Detailed Description
The  function is responsible for allocating and initializing a new  structure with sensible default values. This function creates the fundamental connection object that will later be configured and used to establish connections to PostgreSQL servers.

The function performs several critical initialization tasks:
- Allocates memory for the main PGconn structure
- Initializes all fields to safe default values
- Sets up input/output buffers with optimal sizes (16KB each)
- Configures notice handling hooks
- Initializes error message and work buffers
- On Windows, ensures WSA (Windows Sockets API) is properly initialized

The buffer sizing strategy is designed for performance: 16KB buffers with 8KB threshold for flushing to minimize kernel context switches during data transfer operations.

## Parameters / Member Variables
This function takes no parameters and returns a pointer to the newly created PGconn structure.

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (memory initialization)
  -  (default notice callback)
  -  (default notice processor)
  -  (buffer initialization)
  -  (buffer validation)
  -  (cleanup on failure)
  - Various constants: , , , , etc.

- Called from (representative examples):
  - 
  - 
  - 
  - 

## Notes and Other Information
- Returns NULL if memory allocation fails
- On Windows, performs one-time WSA initialization and does not call WSACleanup()
- Sets initial buffer sizes to 16KB for both input and output buffers
- Initializes row buffer to handle 32 PGdataValue entries initially
- Sets connection status to  until actual connection is established
- All boolean and pointer fields are zero-initialized via MemSet
- Client encoding defaults to SQL_ASCII
- Error verbosity defaults to 
- Socket is initialized to 
- Pipeline status defaults to 
- The function is designed to fail gracefully, calling freePGconn if any allocation fails

## Simplified Source

```c
PGconn *pqMakeEmptyPGconn(void)
{
    PGconn *conn;

#ifdef WIN32
    // Initialize Windows Sockets API once
    static bool wsastartup_done = false;
    if (!wsastartup_done) {
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
            return NULL;
        wsastartup_done = true;
    }
    WSASetLastError(0);
#endif

    // Allocate main connection structure
    conn = (PGconn *) malloc(sizeof(PGconn));
    if (conn == NULL)
        return conn;

    // Zero-initialize all fields
    MemSet(conn, 0, sizeof(PGconn));

    // Set default notice handling
    conn->noticeHooks.noticeRec = defaultNoticeReceiver;
    conn->noticeHooks.noticeProc = defaultNoticeProcessor;

    // Initialize connection state
    conn->status = CONNECTION_BAD;
    conn->asyncStatus = PGASYNC_IDLE;
    conn->pipelineStatus = PQ_PIPELINE_OFF;
    conn->xactStatus = PQTRANS_IDLE;
    conn->options_valid = false;
    conn->nonblocking = false;
    conn->client_encoding = PG_SQL_ASCII;
    conn->std_strings = false;
    conn->default_transaction_read_only = PG_BOOL_UNKNOWN;
    conn->in_hot_standby = PG_BOOL_UNKNOWN;
    conn->scram_sha_256_iterations = SCRAM_SHA_256_DEFAULT_ITERATIONS;
    conn->verbosity = PQERRORS_DEFAULT;
    conn->show_context = PQSHOW_CONTEXT_ERRORS;
    conn->sock = PGINVALID_SOCKET;

    // Allocate I/O buffers (16KB each for performance)
    conn->inBufSize = 16 * 1024;
    conn->inBuffer = (char *) malloc(conn->inBufSize);
    conn->outBufSize = 16 * 1024;
    conn->outBuffer = (char *) malloc(conn->outBufSize);
    conn->rowBufLen = 32;
    conn->rowBuf = (PGdataValue *) malloc(conn->rowBufLen * sizeof(PGdataValue));

    // Initialize expandable buffers
    initPQExpBuffer(&conn->errorMessage);
    initPQExpBuffer(&conn->workBuffer);

    // Check for allocation failures
    if (conn->inBuffer == NULL || conn->outBuffer == NULL ||
        conn->rowBuf == NULL || PQExpBufferBroken(&conn->errorMessage) ||
        PQExpBufferBroken(&conn->workBuffer)) {
        freePGconn(conn);
        conn = NULL;
    }

    return conn;
}
```