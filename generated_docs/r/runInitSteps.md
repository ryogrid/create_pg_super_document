# runInitSteps

## Location
[src/bin/pgbench/pgbench.c:5259-5343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5259-L5343)

## Overview
Executes the specified initialization steps for pgbench database setup, processing each character in the initialization string and invoking the corresponding initialization function.

## Definition
```c
static void runInitSteps(const char *initialize_steps)
```

## Detailed Description
This function is the main coordinator for pgbench database initialization. It processes each character in the initialize_steps string and executes the corresponding initialization operation. The function establishes a database connection, sets up signal handling for graceful cancellation, and then iterates through each step character to perform the requested operations.

The function supports the following initialization steps:
- 'd': Drop existing tables (initDropTables)
- 't': Create tables (initCreateTables)  
- 'g': Generate data using client-side approach (initGenerateDataClientSide)
- 'G': Generate data using server-side approach (initGenerateDataServerSide)
- 'v': Vacuum tables (initVacuum)
- 'p': Create primary keys (initCreatePKeys)
- 'f': Create foreign keys (initCreateFKeys)
- ' ': Spaces are ignored (used as separators)

Each operation is timed and the results are accumulated in a statistics buffer. Upon completion, the function reports the total execution time and a breakdown of time spent on each operation.

## Parameters / Member Variables
- `initialize_steps`: String containing step characters specifying which initialization operations to perform

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer structure for statistics)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize statistics buffer)
  - doConnect (establish database connection)
  - [setup_cancel_handler](../s/setup_cancel_handler.md) (set up signal handling)
  - [SetCancelConn](../S/SetCancelConn.md) (associate connection with cancel handler)
  - pg_time_usec_t (timestamp type)
  - [pg_time_now](../p/pg_time_now.md) (get current timestamp)
  - [initDropTables](../i/initDropTables.md) (drop existing tables)
  - initCreateTables (create pgbench tables)
  - [initGenerateDataClientSide](../i/initGenerateDataClientSide.md) (populate tables client-side)
  - [initGenerateDataServerSide](../i/initGenerateDataServerSide.md) (populate tables server-side)
  - [initVacuum](../i/initVacuum.md) (vacuum tables)
  - [initCreatePKeys](../i/initCreatePKeys.md) (create primary keys)
  - [initCreateFKeys](../i/initCreateFKeys.md) (create foreign keys)
  - PG_TIME_GET_DOUBLE (convert timestamp to double)
  - [ResetCancelConn](../R/ResetCancelConn.md) (reset cancel connection)
  - [PQfinish](../P/PQfinish.md) (close database connection)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup statistics buffer)
- Called from (representative examples):
  - [main](../m/main.md) (during pgbench initialization mode)

## Notes and Other Information
- This function assumes the initialize_steps string has already been validated by checkInitSteps
- Each step is individually timed and the timing information is displayed to the user
- The function handles database connection management and proper cleanup on exit
- Signal handling is set up to allow graceful cancellation during long-running operations
- Spaces in the initialization string are silently ignored, allowing for readable step specifications
- If an invalid step character is encountered (despite validation), the function exits with an error
- The function provides detailed timing feedback showing both individual step times and total execution time
- Located in src/bin/pgbench/pgbench.c:5259-5343

## Simplified Source

```c
static void runInitSteps(const char *initialize_steps)
{
    PQExpBufferData stats;
    PGconn *con;
    const char *step;
    double run_time = 0.0;
    bool first = true;

    initPQExpBuffer(&stats);

    // Establish database connection
    if ((con = doConnect()) == NULL)
        pg_fatal("could not create connection for initialization");

    setup_cancel_handler(NULL);
    SetCancelConn(con);

    // Process each initialization step
    for (step = initialize_steps; *step != '\0'; step++) {
        char *op = NULL;
        pg_time_usec_t start = pg_time_now();

        // Execute the appropriate initialization function
        switch (*step) {
            case 'd':
                op = "drop tables";
                initDropTables(con);
                break;
            case 't':
                op = "create tables";
                initCreateTables(con);
                break;
            case 'g':
                op = "client-side generate";
                initGenerateDataClientSide(con);
                break;
            case 'G':
                op = "server-side generate";
                initGenerateDataServerSide(con);
                break;
            case 'v':
                op = "vacuum";
                initVacuum(con);
                break;
            case 'p':
                op = "primary keys";
                initCreatePKeys(con);
                break;
            case 'f':
                op = "foreign keys";
                initCreateFKeys(con);
                break;
            case ' ':
                break; /* ignore spaces */
            default:
                pg_log_error("unrecognized initialization step \"%c\"", *step);
                PQfinish(con);
                exit(1);
        }

        // Record timing for completed operations
        if (op != NULL) {
            double elapsed_sec = PG_TIME_GET_DOUBLE(pg_time_now() - start);

            if (!first)
                appendPQExpBufferStr(&stats, ", ");
            else
                first = false;

            appendPQExpBuffer(&stats, "%s %.2f s", op, elapsed_sec);
            run_time += elapsed_sec;
        }
    }

    // Report completion statistics
    fprintf(stderr, "done in %.2f s (%s).\n", run_time, stats.data);
    ResetCancelConn();
    PQfinish(con);
    termPQExpBuffer(&stats);
}
```