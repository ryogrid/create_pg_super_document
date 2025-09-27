# BackendInitialize

## Location
[src/backend/tcop/backend_startup.c:122-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L122-L361)

## Overview
BackendInitialize initializes an interactive (postmaster-child) backend process and collects the client's startup packet, handling all pre-authentication setup.

## Definition

```c
structure and all data structures attached to it are allocated
	 * in TopMemoryContext, so that they survive into PostgresMain execution.
	 * We need not worry about leaking this storage on failure, since we
	 * aren't in the postmaster process anymore.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
```
## Detailed Description
BackendInitialize performs comprehensive initialization for a new backend process, including setting up signal handlers, establishing libpq communication, collecting client connection information, processing SSL startup and startup packets, and validating database availability state. The function is designed to work without shared memory access and handles various failure scenarios with appropriate cleanup. It establishes timeout mechanisms to prevent buggy clients from hanging connections indefinitely.

## Parameters / Member Variables
- : Socket connection to the client
- : Connection acceptance state indicating database availability (CAC_OK, CAC_STARTUP, CAC_SHUTDOWN, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ReserveExternalFD](../R/ReserveExternalFD.md)
  - [pg_usleep](../p/pg_usleep.md)
  - [pq_init](../p/pq_init.md)
  - [pqsignal](../p/pqsignal.md)
  - [InitializeTimeouts](../I/InitializeTimeouts.md)
  - sigprocmask
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md)
  - [RegisterTimeout](../R/RegisterTimeout.md)
  - [ProcessSSLStartup](../P/ProcessSSLStartup.md)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md)
  - [disable_timeout](../d/disable_timeout.md)
  - [check_on_shmem_exit_lists_are_empty](../c/check_on_shmem_exit_lists_are_empty.md)
  - [proc_exit](../p/proc_exit.md)
  - [GetBackendTypeDesc](../G/GetBackendTypeDesc.md)
  - init_ps_display
  - [set_ps_display](../s/set_ps_display.md)
- Called from (representative examples):
  - [BackendMain](BackendMain.md)

## Notes and Other Information
- Function does not depend on shared memory access and must not modify shared memory before authentication
- Sets up SIGTERM handler (process_startup_packet_die) that calls _exit(1) to allow clean postmaster shutdown
- Uses AuthenticationTimeout twice: once for startup packet collection, once for authentication operations
- Implements PreAuthDelay debugging feature for attaching debuggers to new backends
- Performs reverse DNS lookup for logging if log_hostname is enabled
- Handles various database states (startup, shutdown, recovery, too many connections) with appropriate error messages
- Sets process title for ps display after collecting user and database information
- Located in src/backend/tcop/backend_startup.c:122-361

## Simplified Source

```c
// Simplified version of BackendInitialize
static void BackendInitialize(ClientSocket *client_sock, CAC_state cac) {
    Port *port;
    char remote_host[NI_MAXHOST];
    char remote_port[NI_MAXSERV];
    StringInfoData ps_data;
    MemoryContext oldcontext;
    int status;

    // Reserve file descriptor for client socket
    ReserveExternalFD();

    // Optional pre-authentication delay for debugging
    if (PreAuthDelay > 0)
        pg_usleep(PreAuthDelay * 1000000L);

    // Mark authentication as in progress
    ClientAuthInProgress = true;

    // Initialize libpq communication in TopMemoryContext
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    port = MyProcPort = pq_init(client_sock);
    MemoryContextSwitchTo(oldcontext);

    // Enable error reporting to client
    whereToSendOutput = DestRemote;

    // Set up signal handlers for startup packet timeout
    pqsignal(SIGTERM, process_startup_packet_die);
    InitializeTimeouts();
    sigprocmask(SIG_SETMASK, &StartupBlockSig, NULL);

    // Get client hostname and port for logging
    remote_host[0] = '\0';
    remote_port[0] = '\0';
    pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
                       remote_host, sizeof(remote_host),
                       remote_port, sizeof(remote_port),
                       (log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV);

    // Save connection info and log if enabled
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    port->remote_host = pstrdup(remote_host);
    port->remote_port = pstrdup(remote_port);

    if (Log_connections) {
        ereport(LOG, (errmsg("connection received: host=%s port=%s",
                             remote_host, remote_port)));
    }

    // Save hostname if reverse DNS lookup was successful
    if (log_hostname && remote_host_is_hostname(remote_host)) {
        port->remote_hostname = pstrdup(remote_host);
    }
    MemoryContextSwitchTo(oldcontext);

    // Set timeout for startup packet collection
    RegisterTimeout(STARTUP_PACKET_TIMEOUT, StartupPacketTimeoutHandler);
    enable_timeout_after(STARTUP_PACKET_TIMEOUT, AuthenticationTimeout * 1000);

    // Handle SSL handshake and startup packet
    status = ProcessSSLStartup(port);
    if (status == STATUS_OK)
        status = ProcessStartupPacket(port, false, false);

    // Check database availability state and reject if necessary
    if (status == STATUS_OK) {
        switch (cac) {
            case CAC_STARTUP:
                ereport(FATAL, (errmsg("the database system is starting up")));
                break;
            case CAC_SHUTDOWN:
                ereport(FATAL, (errmsg("the database system is shutting down")));
                break;
            case CAC_RECOVERY:
                ereport(FATAL, (errmsg("the database system is in recovery mode")));
                break;
            case CAC_TOOMANY:
                ereport(FATAL, (errmsg("sorry, too many clients already")));
                break;
            case CAC_OK:
                break;
        }
    }

    // Disable timeout and restore signal mask
    disable_timeout(STARTUP_PACKET_TIMEOUT, false);
    sigprocmask(SIG_SETMASK, &BlockSig, NULL);

    // Safety check - ensure no shared memory modifications yet
    check_on_shmem_exit_lists_are_empty();

    // Exit if startup packet processing failed
    if (status != STATUS_OK)
        proc_exit(0);

    // Set process title for ps display
    initStringInfo(&ps_data);
    if (am_walsender)
        appendStringInfo(&ps_data, "%s ", GetBackendTypeDesc(B_WAL_SENDER));
    appendStringInfo(&ps_data, "%s ", port->user_name);
    if (port->database_name[0] != '\0')
        appendStringInfo(&ps_data, "%s ", port->database_name);
    appendStringInfoString(&ps_data, port->remote_host);
    if (port->remote_port[0] != '\0')
        appendStringInfo(&ps_data, "(%s)", port->remote_port);

    init_ps_display(ps_data.data);
    pfree(ps_data.data);
    set_ps_display("initializing");
}
```

Key simplifications made:
- Removed detailed error handling for network operations
- Consolidated remote hostname validation logic into conceptual helper
- Simplified CAC_NOTCONSISTENT case handling
- Abstracted complex reverse DNS validation checks
- Removed verbose comments while preserving essential logic flow
- Focused on the main execution path through startup packet processing
- Maintained all critical timeout and signal handling mechanisms