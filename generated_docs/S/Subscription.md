# Subscription

## Location
[src/include/catalog/pg_subscription.h:129-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_subscription.h#L129-L160)

## Overview
The Subscription structure represents a logical replication subscription in PostgreSQL, containing all the configuration parameters and metadata needed to manage replication from a publication on a remote server.

## Definition

```c
typedef struct Subscription
{
	Oid			oid;			/* Oid of the subscription */
	Oid			dbid;			/* Oid of the database which subscription is
								 * in */
	XLogRecPtr	skiplsn;		/* All changes finished at this LSN are
								 * skipped */
	char	   *name;			/* Name of the subscription */
	Oid			owner;			/* Oid of the subscription owner */
	bool		ownersuperuser; /* Is the subscription owner a superuser? */
	bool		enabled;		/* Indicates if the subscription is enabled */
	bool		binary;			/* Indicates if the subscription wants data in
								 * binary format */
	char		stream;			/* Allow streaming in-progress transactions.
								 * See LOGICALREP_STREAM_xxx constants. */
	char		twophasestate;	/* Allow streaming two-phase transactions */
	bool		disableonerr;	/* Indicates if the subscription should be
								 * automatically disabled if a worker error
								 * occurs */
	bool		passwordrequired;	/* Must connection use a password? */
	bool		runasowner;		/* Run replication as subscription owner */
	bool		failover;		/* True if the associated replication slots
								 * (i.e. the main slot and the table sync
								 * slots) in the upstream database are enabled
								 * to be synchronized to the standbys. */
	char	   *conninfo;		/* Connection string to the publisher */
	char	   *slotname;		/* Name of the replication slot */
	char	   *synccommit;		/* Synchronous commit setting for worker */
	List	   *publications;	/* List of publication names to subscribe to */
	char	   *origin;			/* Only publish data originating from the
								 * specified origin */
} Subscription;
```
## Detailed Description
The Subscription structure is a core data structure in PostgreSQL's logical replication system. It encapsulates all the necessary configuration and state information for a subscription that replicates data from one or more publications on a remote PostgreSQL server. This structure is used throughout the logical replication subsystem to manage subscription behavior, connection parameters, and replication settings. The structure supports advanced features like binary format replication, streaming transactions, two-phase commit, failover scenarios, and fine-grained origin filtering.

## Parameters / Member Variables
- : Unique object identifier for this subscription in the system catalogs
- : Object identifier of the database containing this subscription
- : LSN (Log Sequence Number) threshold; changes completed at or before this LSN are skipped during replication
- : Human-readable name of the subscription
- : Object identifier of the user who owns this subscription
- : Boolean flag indicating whether the subscription owner has superuser privileges
- : Boolean flag controlling whether the subscription is actively replicating data
- : Boolean flag indicating whether to request data in binary format from the publisher
- : Character flag controlling streaming of in-progress transactions (see LOGICALREP_STREAM_xxx constants)
- : Character flag controlling streaming of two-phase commit transactions
- : Boolean flag indicating whether to automatically disable the subscription on worker errors
- : Boolean flag indicating whether the connection to the publisher must use a password
- : Boolean flag controlling whether replication runs with subscription owner privileges
- : Boolean flag indicating whether associated replication slots are synchronized to standbys for failover support
- : Connection string containing parameters needed to connect to the publisher database
- : Name of the replication slot on the publisher side
- : Synchronous commit setting that controls transaction durability for the replication worker
- : List of publication names on the publisher that this subscription replicates from
- : Filter string to only replicate data originating from the specified origin node

## Dependencies
- Functions called/Symbols referenced: (Structure definition - no direct function calls)
- Called from (representative examples):
  - [GetSubscription](../G/GetSubscription.md) (retrieves subscription information from catalog)
  - [FreeSubscription](../F/FreeSubscription.md) (deallocates subscription structure memory)
  - [AlterSubscription_refresh](../A/AlterSubscription_refresh.md) (refreshes subscription configuration)
  - [AlterSubscription](../A/AlterSubscription.md) (modifies subscription parameters)
  - get_subscription_list (retrieves list of subscriptions for launcher)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (checks if subscription needs to be reloaded)

## Notes and Other Information
- This structure is primarily used in the logical replication subsystem and subscription management commands
- The structure supports both traditional and streaming replication modes with configurable transaction handling
- Security features include owner privilege checking and password requirements
- The failover capability enables high-availability scenarios with standby servers
- Origin filtering allows selective replication in multi-node topologies
- Memory management for string fields (name, conninfo, slotname, synccommit, origin) and the publications list must be handled carefully to avoid leaks