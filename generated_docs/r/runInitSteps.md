# runInitSteps

## Location
src/bin/pgbench/pgbench.c: 5259 - 5343

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
  - PQExpBufferData (buffer structure for statistics)
  - initPQExpBuffer (initialize statistics buffer)
  - doConnect (establish database connection)
  - setup_cancel_handler (set up signal handling)
  - SetCancelConn (associate connection with cancel handler)
  - pg_time_usec_t (timestamp type)
  - pg_time_now (get current timestamp)
  - initDropTables (drop existing tables)
  - initCreateTables (create pgbench tables)
  - initGenerateDataClientSide (populate tables client-side)
  - initGenerateDataServerSide (populate tables server-side)
  - initVacuum (vacuum tables)
  - initCreatePKeys (create primary keys)
  - initCreateFKeys (create foreign keys)
  - PG_TIME_GET_DOUBLE (convert timestamp to double)
  - ResetCancelConn (reset cancel connection)
  - PQfinish (close database connection)
  - termPQExpBuffer (cleanup statistics buffer)
- Called from (representative examples):
  - main (during pgbench initialization mode)

## Notes and Other Information
- This function assumes the initialize_steps string has already been validated by checkInitSteps
- Each step is individually timed and the timing information is displayed to the user
- The function handles database connection management and proper cleanup on exit
- Signal handling is set up to allow graceful cancellation during long-running operations
- Spaces in the initialization string are silently ignored, allowing for readable step specifications
- If an invalid step character is encountered (despite validation), the function exits with an error
- The function provides detailed timing feedback showing both individual step times and total execution time
- Located in src/bin/pgbench/pgbench.c:5259-5343