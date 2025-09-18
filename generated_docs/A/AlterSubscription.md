# AlterSubscription

## Location
[src/backend/commands/subscriptioncmds.c:1084-1552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1084-L1552)

## Overview
AlterSubscription modifies existing logical replication subscriptions, handling various types of changes including options, connection settings, publications, and enabling/disabling subscriptions.

## Definition


## Detailed Description
AlterSubscription is the main function for handling ALTER SUBSCRIPTION SQL commands in PostgreSQL's logical replication system. It performs comprehensive validation and modification of subscription properties stored in the pg_subscription system catalog.

The function handles multiple alteration types through a switch statement:
- ALTER_SUBSCRIPTION_OPTIONS: Modifies subscription options like slot_name, binary, streaming, failover, etc.
- ALTER_SUBSCRIPTION_ENABLED: Enables or disables the subscription
- ALTER_SUBSCRIPTION_CONNECTION: Updates connection string to the publisher
- ALTER_SUBSCRIPTION_SET_PUBLICATION: Changes the list of publications
- ALTER_SUBSCRIPTION_ADD_PUBLICATION/DROP_PUBLICATION: Adds or removes individual publications
- ALTER_SUBSCRIPTION_REFRESH: Refreshes subscription table list from publisher
- ALTER_SUBSCRIPTION_SKIP: Sets LSN to skip problematic transactions

The function enforces security restrictions, preventing non-superusers from modifying subscriptions with password_required=false. It also validates state dependencies, such as preventing certain operations on enabled subscriptions and ensuring proper transaction block handling for operations that cannot be rolled back.

## Parameters / Member Variables
- : Parser state for processing subscription options and validating syntax
- : ALTER SUBSCRIPTION statement containing the specific alteration type and parameters
- : Boolean flag indicating if this is a top-level command, used for transaction block validation

## Dependencies
- Functions called/Symbols referenced:
  - [GetSubscription](../G/GetSubscription.md): Retrieves subscription details from catalog
  - [parse_subscription_options](../p/parse_subscription_options.md): Parses and validates subscription option changes
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents operations that can't be rolled back from running in transaction blocks
  - [publicationListToArray](../p/publicationListToArray.md): Converts publication name list to array format
  - [AlterSubscription_refresh](AlterSubscription_refresh.md): Handles subscription refresh operations
  - walrcv_alter_slot: Alters replication slot properties on publisher
  - [heap_freetuple](../h/heap_freetuple.md): Frees heap tuple memory
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processor in tcop/utility.c:1863

## Notes and Other Information
- Requires exclusive lock on the subscription to prevent concurrent modifications
- Enforces ownership checks and superuser restrictions for security-sensitive options
- Handles complex state validations for two-phase commit and failover scenarios  
- Some operations like failover changes cannot be rolled back and must be prevented in transaction blocks
- Automatically wakes up replication workers when changes require immediate processing
- Connection to publisher is established only when necessary (e.g., for slot alteration)