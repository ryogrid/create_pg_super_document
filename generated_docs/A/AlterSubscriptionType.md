# AlterSubscriptionType

## Location
src/include/nodes/parsenodes.h: 4213 - 4214

## Overview
AlterSubscriptionType is an enumeration that specifies the type of alteration operation to perform on a PostgreSQL logical replication subscription.

## Definition


## Detailed Description
This enumeration defines the different types of modifications that can be made to PostgreSQL logical replication subscriptions through the ALTER SUBSCRIPTION statement. Subscriptions represent the subscriber side of logical replication, connecting to publications on remote PostgreSQL instances. Each enum value represents a specific aspect of the subscription that can be modified, from connection parameters to publication lists and operational settings.

## Parameters / Member Variables
- : Modify subscription options (e.g., synchronous_commit, binary, streaming)
- : Change the connection string to the publisher
- : Replace the entire list of publications that this subscription follows
- : Add new publications to the existing subscription list
- : Remove specific publications from the subscription
- : Refresh the subscription (re-synchronize table list from publications)
- : Enable or disable the subscription
- : Skip to a specific LSN (Log Sequence Number) for error recovery

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - AlterSubscriptionStmt (as the 'kind' field)
  - Parser grammar rules in gram.y for ALTER SUBSCRIPTION statements
  - [Subscription](../S/Subscription.md) command functions in src/backend/commands/subscriptioncmds.c

## Notes and Other Information
- This enum is part of PostgreSQL's logical replication infrastructure
- Used specifically in ALTER SUBSCRIPTION statements with various clauses
- The SKIP option is primarily used for error recovery when replication encounters issues
- The REFRESH operation is important when publications change their table lists
- Works in conjunction with AlterSubscriptionStmt structure to represent parsed ALTER SUBSCRIPTION commands
- Located in src/include/nodes/parsenodes.h as part of the SQL parsing framework
- Logical replication subscriptions were introduced in PostgreSQL 10 as part of the built-in logical replication feature