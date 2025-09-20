# setup_publisher

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:734-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L734-L812)

## Overview
Creates publications and replication slots in preparation for logical replication, returning the LSN from the latest replication slot which serves as the replication start point.

## Definition

```c
static char *
setup_publisher(struct LogicalRepInfo *dbinfo)
```
## Detailed Description
This function prepares the publisher side for logical replication by creating necessary publications and replication slots across multiple databases. It iterates through all configured databases, connects to each one, and performs the following operations:

1. Generates object names if they weren't provided via command-line options
2. Creates publications on the publisher (executed before promoting subscriber to avoid transaction visibility issues)
3. Creates logical replication slots on the publisher
4. Writes an additional WAL record to ensure recovery completes properly on idle systems

The function returns the LSN from the last created replication slot, which will be used as the recovery_target_lsn for the subscriber.

## Parameters / Member Variables
- : Array of LogicalRepInfo structures containing database connection information and object names for each database

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_seed
  - connect_database
  - [generate_object_name](../g/generate_object_name.md)
  - [create_publication](../c/create_publication.md)
  - [pg_free](../p/pg_free.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - pg_log_info
  - [PQexec](../P/PQexec.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [disconnect_database](../d/disconnect_database.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- The function processes databases sequentially but maintains the LSN from the last replication slot created
- For the last database, it executes pg_log_standby_snapshot() to write a harmless WAL record, preventing indefinite waits during recovery on idle systems
- Object names are auto-generated if not provided via command-line options
- The replication slot name defaults to the subscription name if not explicitly specified
- Publications are created before replication slots to ensure proper transaction visibility