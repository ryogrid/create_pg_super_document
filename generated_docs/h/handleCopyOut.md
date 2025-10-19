# handleCopyOut

## Location
[src/bin/psql/copy.c:434-507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L434-L507)

## Overview
Handles the client-side processing of COPY TO STDOUT operations by reading data from a PostgreSQL connection and writing it to a specified output stream.

## Definition
```c
bool
handleCopyOut(PGconn *conn, FILE *copystream, PGresult **res)
```

## Detailed Description
This function processes the data transfer phase of a COPY TO STDOUT command. After the backend has been issued a COPY TO command and returned PGRES_COPY_OUT status, this function continuously reads data chunks from the connection using PQgetCopyData, writes them to the output stream, and handles proper cleanup and error reporting. The function manages the complete data transfer loop until completion or error, handles stream flushing and error conditions, and retrieves the final command result status.

## Parameters / Member Variables
- `conn`: Active database connection that has issued a COPY TO command
- `copystream`: Output file stream to write the copied data (can be NULL to discard data)
- `res`: Pointer to store the final PGresult from the COPY operation

## Dependencies
- Functions called/Symbols referenced:
  - [PQgetCopyData](../P/PQgetCopyData.md) (libpq function to read COPY data)
  - [PQfreemem](../P/PQfreemem.md) (libpq memory management)
  - [PQgetResult](../P/PQgetResult.md) (libpq result retrieval)
  - [PQerrorMessage](../P/PQerrorMessage.md) (libpq error reporting)
  - [PQresultStatus](../P/PQresultStatus.md) (libpq result status)
  - PGRES_COMMAND_OK (libpq status constant)
  - pg_log_error, pg_log_info (PostgreSQL logging)
  - fwrite, fflush (standard I/O)
- Called from (representative examples):
  - [HandleCopyResult](../H/HandleCopyResult.md) (src/bin/psql/common.c:915)

## Notes and Other Information
- Returns true on success, false on any error condition
- Can handle NULL copystream to consume data without writing (useful for testing or discarding output)
- Implements robust error handling with detailed error messages
- Continues reading data even after write errors to avoid leaving the connection in an inconsistent state
- Part of the standard PostgreSQL client-side COPY implementation that can be reused in other applications
- Handles libpq memory management properly by calling PQfreemem for each data buffer
- Provides comprehensive error reporting for network, write, and command execution errors
- Critical component of psql's COPY TO functionality

## Simplified Source

```c
bool handleCopyOut(PGconn *conn, FILE *copystream, PGresult **res) {
    bool success = true;
    char *data_buffer;
    int bytes_read;

    // Main data transfer loop: read chunks and write to output
    for (;;) {
        bytes_read = PQgetCopyData(conn, &data_buffer, 0);

        if (bytes_read < 0)
            break;  // Transfer complete or error occurred

        if (data_buffer) {
            // Write data to output stream (if provided)
            if (success && copystream &&
                fwrite(data_buffer, 1, bytes_read, copystream) != bytes_read) {
                pg_log_error("could not write COPY data: %m");
                success = false;  // Continue reading to avoid connection issues
            }
            PQfreemem(data_buffer);
        }
    }

    // Flush output stream
    if (success && copystream && fflush(copystream)) {
        pg_log_error("could not write COPY data: %m");
        success = false;
    }

    // Check for transfer errors
    if (bytes_read == -2) {
        pg_log_error("COPY data transfer failed: %s", PQerrorMessage(conn));
        success = false;
    }

    // Get final command status
    *res = PQgetResult(conn);
    if (PQresultStatus(*res) != PGRES_COMMAND_OK) {
        pg_log_info("%s", PQerrorMessage(conn));
        success = false;
    }

    return success;
}
```