# AlterSubscriptionType

## Location
[src/include/nodes/parsenodes.h:4213-4214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4213-L4214)

## Overview
AlterSubscriptionType is an enumeration that specifies the type of alteration operation to perform on a PostgreSQL logical replication subscription.

## Definition

```c
typedef struct AlterSubscriptionStmt
{
	NodeTag		type;
	AlterSubscriptionType kind; /* ALTER_SUBSCRIPTION_OPTIONS, etc */
	char	   *subname;		/* Name of the subscription */
	char	   *conninfo;		/* Connection string to publisher */
	List	   *publication;	/* One or more publication to subscribe to */
	List	   *options;		/* List of DefElem nodes */
} AlterSubscriptionStmt;
```
## Detailed Description
This enumeration defines the different types of modifications that can be made to PostgreSQL logical replication subscriptions through the ALTER SUBSCRIPTION statement. Subscriptions represent the subscriber side of logical replication, connecting to publications on remote PostgreSQL instances. Each enum value represents a specific aspect of the subscription that can be modified, from connection parameters to publication lists and operational settings.

## Parameters / Member Variables
- `ALTER_SUBSCRIPTION_OPTIONS`: Modify subscription options (e.g., synchronous_commit, binary, streaming)
- `ALTER_SUBSCRIPTION_CONNECTION`: Change the connection string to the publisher
- `ALTER_SUBSCRIPTION_SET_PUBLICATION`: Replace the entire list of publications that this subscription follows
- `ALTER_SUBSCRIPTION_ADD_PUBLICATION`: Add new publications to the existing subscription list
- `ALTER_SUBSCRIPTION_DROP_PUBLICATION`: Remove specific publications from the subscription
- `ALTER_SUBSCRIPTION_REFRESH`: Refresh the subscription (re-synchronize table list from publications)
- `ALTER_SUBSCRIPTION_ENABLED`: Enable or disable the subscription
- `ALTER_SUBSCRIPTION_SKIP`: Skip to a specific LSN (Log Sequence Number) for error recovery

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - [AlterSubscriptionStmt](AlterSubscriptionStmt.md) (as the 'kind' field)
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