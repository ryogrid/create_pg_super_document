# check_subscriber

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:961-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L961-L1061)

## Overview
Validates that the standby server is properly configured and ready to become a logical replication subscriber by checking essential parameters and confirming recovery status.

## Definition


## Detailed Description
This function performs comprehensive validation of the subscriber (standby server) to ensure it can support logical replication after promotion. It verifies critical prerequisites and configuration parameters:

1. Confirms the server is currently in recovery mode (must be a standby)
2. Validates sufficient replication slots are available for all databases
3. Ensures adequate logical replication workers are configured
4. Verifies sufficient worker processes are available (requires num_dbs + 1)
5. Extracts primary_slot_name if configured for physical replication

The function connects to the subscriber using the first database connection info and performs parameter validation to prevent runtime failures during logical replication setup.

## Parameters / Member Variables
- : Array of LogicalRepInfo structures containing database connection information, uses the first entry for subscriber validation

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - connect_database
  - [server_is_in_recovery](../s/server_is_in_recovery.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [PQexec](../P/PQexec.md)
  - PGRES_TUPLES_OK
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - atoi
  - [PQgetvalue](../P/PQgetvalue.md)
  - strcmp
  - [pg_strdup](../p/pg_strdup.md)
  - pg_log_debug
  - [PQclear](../P/PQclear.md)
  - pg_log_error
  - pg_log_error_hint
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Must be called before promoting the standby to ensure logical replication compatibility
- Terminates the program if validation fails
- Sets global primary_slot_name variable if physical replication slot is configured
- Worker process requirement includes one additional process beyond the number of databases
- Cannot reliably detect cascaded replication scenarios that would be broken by pg_resetwal
- Critical prerequisite check executed early in the conversion process