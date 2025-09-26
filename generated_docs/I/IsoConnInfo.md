# IsoConnInfo

## Location
src/test/isolation/isolationtester.c: 25 - 38

## Overview
IsoConnInfo is a structure that represents connection information for PostgreSQL isolation testing, managing individual database connections within the isolation test framework.

## Definition


## Detailed Description
IsoConnInfo serves as the central data structure for managing database connections in PostgreSQL's isolation testing framework. Each instance represents a single database connection that can execute test steps as part of isolation test scenarios. The structure tracks both the connection state and execution context, enabling the isolation tester to coordinate multiple concurrent sessions and monitor their interactions.

The isolation tester uses an array of IsoConnInfo structures, where conns[0] is reserved for the global setup, teardown, and watchdog connection, while additional connections (conns[1], conns[2], etc.) represent spec-defined test sessions. This design allows the framework to simulate concurrent database operations and test transaction isolation levels.

## Parameters / Member Variables
- : The libpq PGconn connection object that represents the actual database connection
- : The numeric backend process ID of the PostgreSQL server process handling this connection
- : String representation of the backend PID for display and logging purposes
- : Human-readable name of the session associated with this connection (as defined in test specifications)
- : Pointer to the currently executing PermutationStep on this connection, or NULL if the connection is idle
- : Counter tracking the total number of NOTICE messages received from this connection during test execution

## Dependencies
- Functions called/Symbols referenced:
  - PermutationStep (referenced as a pointer type for tracking active execution steps)
  - PGconn (libpq connection type)

- Called from (representative examples):
  - main (at src/test/isolation/isolationtester.c:150)
  - run_permutation (at src/test/isolation/isolationtester.c:576, 653)
  - try_complete_step (at src/test/isolation/isolationtester.c:821)
  - step_has_blocker (at src/test/isolation/isolationtester.c:1087)
  - isotesterNoticeProcessor (at src/test/isolation/isolationtester.c:1128)

## Notes and Other Information
- The IsoConnInfo structure is defined in the isolation testing framework (src/test/isolation/isolationtester.c:25-38)
- This is part of PostgreSQL's testing infrastructure specifically for isolation and concurrency testing
- The structure is used in conjunction with the isolation test specification language to verify transaction isolation behavior
- The backend PID tracking enables the framework to monitor and coordinate between different backend processes
- Notice message counting helps verify expected database notifications during test execution
- The connection at index 0 (conns[0]) has a special role as the global connection for setup, teardown, and watchdog operations