# BackendInitialize

## Location
src/backend/tcop/backend_startup.c: 122 - 361

## Overview
BackendInitialize initializes an interactive (postmaster-child) backend process and collects the client's startup packet, handling all pre-authentication setup.

## Definition


## Detailed Description
BackendInitialize performs comprehensive initialization for a new backend process, including setting up signal handlers, establishing libpq communication, collecting client connection information, processing SSL startup and startup packets, and validating database availability state. The function is designed to work without shared memory access and handles various failure scenarios with appropriate cleanup. It establishes timeout mechanisms to prevent buggy clients from hanging connections indefinitely.

## Parameters / Member Variables
- : Socket connection to the client
- : Connection acceptance state indicating database availability (CAC_OK, CAC_STARTUP, CAC_SHUTDOWN, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ReserveExternalFD
  - pg_usleep
  - pq_init
  - pqsignal
  - InitializeTimeouts
  - sigprocmask
  - pg_getnameinfo_all
  - RegisterTimeout
  - ProcessSSLStartup
  - ProcessStartupPacket
  - disable_timeout
  - check_on_shmem_exit_lists_are_empty
  - proc_exit
  - GetBackendTypeDesc
  - init_ps_display
  - set_ps_display
- Called from (representative examples):
  - BackendMain

## Notes and Other Information
- Function does not depend on shared memory access and must not modify shared memory before authentication
- Sets up SIGTERM handler (process_startup_packet_die) that calls _exit(1) to allow clean postmaster shutdown
- Uses AuthenticationTimeout twice: once for startup packet collection, once for authentication operations
- Implements PreAuthDelay debugging feature for attaching debuggers to new backends
- Performs reverse DNS lookup for logging if log_hostname is enabled
- Handles various database states (startup, shutdown, recovery, too many connections) with appropriate error messages
- Sets process title for ps display after collecting user and database information
- Located in src/backend/tcop/backend_startup.c:122-361