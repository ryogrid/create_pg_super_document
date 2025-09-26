# CreateSubscriptionStmt

## Location
[src/include/nodes/parsenodes.h:4194-4201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4194-L4201)

## Overview
A parse tree node structure representing a CREATE SUBSCRIPTION SQL statement, which establishes a new subscription for logical replication in PostgreSQL to receive data from remote publications.

## Definition

```c
typedef struct CreateSubscriptionStmt
{
	NodeTag		type;
	char	   *subname;		/* Name of the subscription */
	char	   *conninfo;		/* Connection string to publisher */
	List	   *publication;	/* One or more publication to subscribe to */
	List	   *options;		/* List of DefElem nodes */
} CreateSubscriptionStmt;
```
## Detailed Description
CreateSubscriptionStmt represents the parsed form of a CREATE SUBSCRIPTION statement used in PostgreSQL's logical replication system. This structure captures all the essential components needed to establish a subscription that will receive and apply data changes from one or more publications on a remote PostgreSQL server (publisher).

A subscription acts as the consumer side of logical replication, connecting to a publisher database, retrieving the initial data snapshot (if enabled), and continuously applying ongoing changes from the subscribed publications. The subscription maintains its own replication slot on the publisher to track the replication progress.

The structure supports subscription to multiple publications from the same publisher, allowing fine-grained control over which data changes are replicated to the subscriber database.

## Parameters / Member Variables
- : Standard NodeTag identifier for the parse tree node system
- : String containing the name of the subscription being created; must be unique within the current database
- : PostgreSQL connection string specifying how to connect to the publisher database; contains host, port, database name, user credentials, and other connection parameters
- : List of string values representing the names of publications on the publisher database to subscribe to; must contain at least one publication name
- : List of DefElem nodes representing subscription options such as 'connect' (whether to connect immediately), 'enabled' (whether subscription is active), 'create_slot' (whether to create replication slot), 'slot_name', 'synchronous_commit', 'binary', 'streaming', 'two_phase', etc.

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (standard parse tree node identifier)
  - [List](../L/List.md) (PostgreSQL's generic list structure for publication names and options)
  - [DefElem](../D/DefElem.md) (definition element for subscription options)
- Called from (representative examples):
  - [CreateSubscription](CreateSubscription.md) (in subscriptioncmds.c for subscription creation and execution)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in utility.c for statement processing)

## Notes and Other Information
- Creating a subscription requires superuser privileges or a role with CREATE privilege on the database
- The connection string must allow the subscription to connect to the publisher with replication privileges
- Default behavior creates and uses a replication slot with the same name as the subscription
- Subscriptions can be created in a disabled state (enabled=false) and activated later
- The initial data synchronization can be controlled via the 'copy_data' option
- Binary transfer mode can be enabled for better performance with large data volumes
- Two-phase commit support allows participation in distributed transactions
- Streaming mode enables applying changes before transaction commit for large transactions
- The subscription worker processes are automatically managed by the subscription infrastructure
- Publications specified must exist on the publisher at the time of subscription creation or initial connection