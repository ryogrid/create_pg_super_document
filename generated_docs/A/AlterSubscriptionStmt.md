# AlterSubscriptionStmt

## Location
[src/include/nodes/parsenodes.h:4215-4223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L4215-L4223)

## Overview
A parse tree node structure representing an ALTER SUBSCRIPTION SQL statement, which modifies various aspects of an existing subscription in PostgreSQL's logical replication system.

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
AlterSubscriptionStmt represents the parsed form of ALTER SUBSCRIPTION statements, which allow modification of existing subscriptions in PostgreSQL's logical replication framework. This structure supports various types of subscription alterations through the  field, including:

- **Options modification**: Change subscription behavior settings (connect, enabled, slot_name, etc.)
- **Connection changes**: Update the connection string to the publisher
- **Publication management**: Add, drop, or replace the set of subscribed publications
- **Refresh operations**: Refresh the subscription to synchronize schema changes
- **Enable/disable operations**: Control subscription activity
- **Skip operations**: Skip problematic transactions during replication

The structure is designed to handle all ALTER SUBSCRIPTION variants using a single parse tree node, with different fields being relevant depending on the specific alteration type.

## Parameters / Member Variables
- : Standard NodeTag identifier for the parse tree node system
- : AlterSubscriptionType enum value specifying the type of alteration being performed (OPTIONS, CONNECTION, SET_PUBLICATION, ADD_PUBLICATION, DROP_PUBLICATION, REFRESH, ENABLED, SKIP)
- : String containing the name of the subscription being altered; must reference an existing subscription
- : New connection string for ALTER SUBSCRIPTION CONNECTION operations; specifies updated publisher connection parameters
- : List of publication names for publication-related operations (ADD, DROP, SET); contains the publications to be added, removed, or set as the complete list
- : List of DefElem nodes containing subscription options for various alter operations; specific options vary by operation type

## Dependencies
- Functions called/Symbols referenced:
  - [AlterSubscriptionType](AlterSubscriptionType.md) (enum defining the type of subscription alteration)
  - NodeTag (standard parse tree node identifier)
  - [List](../L/List.md) (PostgreSQL's generic list structure)
  - [DefElem](../D/DefElem.md) (definition element for options)
- Called from (representative examples):
  - [AlterSubscription](AlterSubscription.md) (in subscriptioncmds.c for statement execution)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in utility.c for statement processing)

## Notes and Other Information
- Different ALTER SUBSCRIPTION operations use different combinations of the structure fields:
  - OPTIONS: uses options field only
  - CONNECTION: uses conninfo field only
  - SET_PUBLICATION: uses publication field, optional WITH options
  - ADD_PUBLICATION: uses publication field for publications to add
  - DROP_PUBLICATION: uses publication field for publications to remove
  - REFRESH: may use options for refresh-specific parameters
  - ENABLED: uses options to specify enabled/disabled state
  - SKIP: uses options to specify LSN or transaction details to skip
- ALTER SUBSCRIPTION requires ownership of the subscription or superuser privileges
- Connection string changes take effect on the next connection attempt
- [Publication](../P/Publication.md) changes may require REFRESH PUBLICATION to synchronize table lists
- The REFRESH operation can be used to handle schema changes in subscribed tables
- SKIP operations are typically used for error recovery when replication encounters problematic transactions
- Some operations may require the subscription to be disabled before execution
- Changes to critical subscription parameters may require restarting the subscription worker processes