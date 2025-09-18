# PostgresMain

## Location
src/backend/tcop/postgres.c: 4239 - 5025

## Overview
The central main loop function for all PostgreSQL backend processes that handles client communication, command processing, and transaction management for both interactive and WAL sender backends.

## Definition


## Detailed Description
PostgresMain is the heart of PostgreSQL's backend processing system. It serves as the main loop for all backend processes, whether they are regular client-serving backends or WAL sender processes. The function is responsible for the complete lifecycle of backend operations including initialization, signal handling, command processing, error recovery, and cleanup.

The function operates in several key phases:
1. **Signal Setup**: Configures signal handlers for various system and PostgreSQL-specific signals, with different handling for WAL senders vs. regular backends
2. **Initialization**: Performs base initialization and database connection setup through InitPostgres()
3. **Main Processing Loop**: An infinite loop that handles client commands using the PostgreSQL frontend/backend protocol
4. **Error Recovery**: Uses setjmp/longjmp for exception handling and transaction abort/recovery
5. **Protocol Handling**: Processes various message types (Query, Parse, Bind, Execute, etc.) according to the PostgreSQL wire protocol

The function includes sophisticated timeout management for idle sessions and idle-in-transaction states, comprehensive error handling and reporting, and support for both simple and extended query protocols. It also handles special cases like WAL sender operations and replication commands.

## Parameters / Member Variables
- : Name of the database to connect to for this backend session
- : PostgreSQL username to be used for authentication and session context

## Dependencies
- Functions called/Symbols referenced:
  - SetProcessingMode (set backend processing mode)
  - WalSndSignals/pqsignal (signal handler setup)
  - BaseInit (basic backend initialization)
  - InitPostgres (database connection and initialization)
  - ReadCommand (read client commands from network)
  - exec_simple_query/exec_parse_message/exec_bind_message/exec_execute_message (command execution)
  - forbidden_in_wal_sender (check if command is allowed in WAL sender)
  - AbortCurrentTransaction (transaction abort and cleanup)
  - pgstat_report_activity (activity reporting for monitoring)
  - EventTriggerOnLogin (login event trigger processing)
- Called from (representative examples):
  - BackendMain (in src/backend/tcop/backend_startup.c:105)
  - PostgresSingleUserMain (in src/backend/tcop/postgres.c:4223)

## Notes and Other Information
- Uses setjmp/longjmp for error recovery rather than PG_TRY/PG_CATCH to maintain exception handling during error recovery itself
- Handles both simple query protocol (PqMsg_Query) and extended query protocol (Parse/Bind/Execute/Describe)
- Includes special handling for WAL sender processes with different signal handlers and command restrictions
- Implements sophisticated timeout management including idle-session and idle-in-transaction timeouts
- Memory management through MessageContext which is reset after each command cycle
- Supports Valgrind integration for memory debugging
- Critical for PostgreSQL's multi-process architecture - each client connection runs this function in its own backend process
- The function never returns under normal circumstances - it either processes commands indefinitely or exits via proc_exit()
- Includes comprehensive protocol violation detection and error reporting