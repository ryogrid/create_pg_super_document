# SubOpts

## Location
src/backend/commands/subscriptioncmds.c: 83 - 102

## Overview
SubOpts is a structure that holds a bitmap representing user-provided CREATE/ALTER SUBSCRIPTION command options and their parsed/default values in PostgreSQL's logical replication system.

## Definition


## Detailed Description
The SubOpts structure serves as a comprehensive container for subscription configuration options in PostgreSQL's logical replication framework. It maintains both a bitmap () to track which options were explicitly provided by the user and the actual values for each configurable parameter. This design allows the system to distinguish between explicitly set options and those using default values, which is crucial for proper subscription management and ALTER SUBSCRIPTION operations.

## Parameters / Member Variables
- : Bitmap tracking which subscription options were explicitly specified by the user
- : Name of the replication slot to be used for the subscription
- : Synchronous commit setting for the subscription
- : Whether to connect to the publisher immediately upon creation
- : Whether the subscription is enabled and should start replicating
- : Whether to create a new replication slot on the publisher
- : Whether to copy existing data from the publisher tables
- : Whether to refresh the subscription's publication list
- : Whether to use binary format for data transfer
- : Streaming mode configuration for large transactions
- : Whether to enable two-phase commit for the subscription
- : Whether to disable the subscription on replication errors
- : Whether password authentication is required
- : Whether to run the subscription as the table owner
- : Whether the subscription supports failover scenarios
- : Origin specification for the subscription
- : Log Sequence Number for subscription positioning

## Dependencies
- Functions called/Symbols referenced:
  - bits32
  - XLogRecPtr
- Called from (representative examples):
  - [parse_subscription_options](../p/parse_subscription_options.md)
  - [CreateSubscription](../C/CreateSubscription.md)
  - [AlterSubscription](../A/AlterSubscription.md)

## Notes and Other Information
This structure is central to PostgreSQL's subscription management system, providing a unified way to handle all subscription configuration parameters. The bitmap approach for tracking specified options ensures that ALTER SUBSCRIPTION commands can properly distinguish between options that should be changed versus those that should retain their current values. The structure supports both basic replication features and advanced capabilities like two-phase commit, binary transfer, and failover scenarios.