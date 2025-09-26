# DropSubscriptionStmt

## Location
src/include/nodes/parsenodes.h: 4225 - 4231

## Overview
DropSubscriptionStmt represents a parsed DROP SUBSCRIPTION statement in PostgreSQL's logical replication system, encapsulating the necessary information to remove a subscription including error handling behavior and cascading options.

## Definition


## Detailed Description
The DropSubscriptionStmt structure is used to represent a DROP SUBSCRIPTION SQL statement after it has been parsed. It contains all the information needed to execute the subscription removal operation, including the subscription name, error handling preferences, and drop behavior options.

This structure is part of PostgreSQL's logical replication infrastructure, which allows for streaming changes from a publisher database to subscriber databases. When a subscription is dropped, it involves complex cleanup operations including:
- Stopping all associated logical replication workers
- Cleaning up replication origins and slots
- Removing catalog entries
- Optionally dropping replication slots at the publisher node

The structure provides flexibility in how errors are handled (via missing_ok) and whether dependent objects should be automatically removed (via behavior).

## Parameters / Member Variables
- : NodeTag identifying this as a DropSubscriptionStmt node in PostgreSQL's parse tree
- : The name of the subscription to be dropped (as specified in the DROP SUBSCRIPTION statement)
- : Boolean flag indicating whether to suppress errors if the subscription doesn't exist (corresponds to IF EXISTS clause)
- : Specifies drop behavior - either DROP_RESTRICT (fail if dependent objects exist) or DROP_CASCADE (automatically drop dependent objects)

## Dependencies
- Functions called/Symbols referenced:
  - DropBehavior (enum type)
- Called from (representative examples):
  - DropSubscription (main execution function in subscriptioncmds.c:1553)
  - ProcessUtilitySlow (utility command dispatcher in utility.c:1869)

## Notes and Other Information
- This structure is defined in src/include/nodes/parsenodes.h at lines 4225-4231
- The actual subscription dropping logic is implemented in the DropSubscription() function in src/backend/commands/subscriptioncmds.c
- DROP SUBSCRIPTION is a non-transactional operation when it involves dropping replication slots, requiring special handling to prevent execution within transaction blocks
- The missing_ok field corresponds to the optional IF EXISTS clause in the SQL syntax
- The DropBehavior enum supports RESTRICT (default) and CASCADE options, though CASCADE behavior for subscriptions may have specific implications for dependent replication objects
- This structure is part of the larger parse tree infrastructure used throughout PostgreSQL for representing SQL statements