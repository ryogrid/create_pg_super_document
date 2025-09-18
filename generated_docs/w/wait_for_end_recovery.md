# wait_for_end_recovery

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1511-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1511-L1562)

## Overview
wait_for_end_recovery is a function that waits for a PostgreSQL server to complete its recovery process, ensuring it reaches a consistent state before proceeding with subscription creation operations.

## Definition


## Detailed Description
This function is part of the pg_createsubscriber utility and monitors a PostgreSQL server to detect when it finishes the recovery process. It repeatedly checks the server's recovery status by connecting to it and querying whether it is still in recovery mode. The function will wait indefinitely by default, but can be configured with a timeout through the recovery_timeout option. If a timeout is specified and exceeded, the function will terminate the standby server and exit abnormally. In dry run mode, the function immediately considers recovery as ended since no actual recovery is performed.

The function implements a polling mechanism with a fixed wait interval (WAIT_INTERVAL seconds) between status checks. It provides logging information to keep users informed about the recovery progress and includes helpful hints about recovery failure scenarios.

## Parameters / Member Variables
- : Connection string used to connect to the target PostgreSQL server
- : Pointer to CreateSubscriberOptions structure containing configuration options, particularly the recovery_timeout setting

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - connect_database
  - [server_is_in_recovery](../s/server_is_in_recovery.md)
  - [stop_standby_server](../s/stop_standby_server.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [pg_usleep](../p/pg_usleep.md)
  - pg_log_info_hint
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_createsubscriber)

## Notes and Other Information
- The function uses a global variable  to track recovery completion status
- It respects dry run mode by immediately considering recovery as completed
- The timeout mechanism provides a safety valve to prevent indefinite waiting
- Recovery timeout is specified in seconds through the CreateSubscriberOptions structure
- The function provides user-friendly logging and hints about potential failure scenarios
- After successful recovery completion, it warns users that failure beyond this point requires recreating the physical replica