# LogicalRepInfo

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 49 - 110

## Overview
LogicalRepInfo is a struct that holds runtime information and configuration details for individual database logical replication setups within the pg_createsubscriber utility.

## Definition


## Detailed Description
LogicalRepInfo serves as a per-database container for logical replication configuration and state tracking in the pg_createsubscriber utility. This structure encapsulates all the necessary information required to manage logical replication for a single database during the conversion process from standby to subscriber. It stores connection strings, object names, and boolean flags that track whether certain replication objects were created during the conversion process. The structure is used extensively throughout the pg_createsubscriber.c module to manage the setup, configuration, and cleanup of logical replication components for each database being converted.

## Parameters / Member Variables
- : Name of the database for which logical replication is being configured
- : Connection string used to connect to the publisher database server for this specific database
- : Connection string used to connect to the subscriber database server for this specific database
- : Name of the publication on the publisher side that will be replicated for this database
- : Name of the subscription on the subscriber side that will consume data from the publication
- : Name of the replication slot used for logical replication between publisher and subscriber
- : Boolean flag indicating whether the replication slot was created during the conversion process (used for cleanup)
- : Boolean flag indicating whether the publication was created during the conversion process (used for cleanup)

## Dependencies
- Functions called/Symbols referenced:
  - [CreateSubscriberOptions](../C/CreateSubscriberOptions.md) (referenced in function signatures that use LogicalRepInfo)
- Called from (representative examples):
  - store_pub_sub_info (src/bin/pg_basebackup/pg_createsubscriber.c:437)
  - [setup_publisher](../s/setup_publisher.md) (src/bin/pg_basebackup/pg_createsubscriber.c:734)
  - [check_publisher](../c/check_publisher.md) (src/bin/pg_basebackup/pg_createsubscriber.c:841)
  - [check_subscriber](../c/check_subscriber.md) (src/bin/pg_basebackup/pg_createsubscriber.c:961)
  - [setup_subscriber](../s/setup_subscriber.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1143)
  - [setup_recovery](../s/setup_recovery.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1183)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1324)
  - [create_publication](../c/create_publication.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1563)
  - [create_subscription](../c/create_subscription.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1691)
  - [set_replication_progress](../s/set_replication_progress.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1749)
  - [enable_subscription](../e/enable_subscription.md) (src/bin/pg_basebackup/pg_createsubscriber.c:1840)

## Notes and Other Information
LogicalRepInfo is specifically designed for the pg_createsubscriber utility and works closely with CreateSubscriberOptions. While CreateSubscriberOptions holds command-line configuration parameters, LogicalRepInfo manages the runtime state and database-specific information for each database being converted. The boolean flags (made_replslot and made_publication) are crucial for proper cleanup in case of errors during the conversion process, ensuring that only objects created by the utility are removed if the operation fails. This structure enables the utility to handle multiple databases simultaneously, with each LogicalRepInfo instance representing one database's logical replication setup.