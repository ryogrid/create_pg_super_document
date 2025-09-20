# AlterSubscriptionOwner

## Location
[src/backend/commands/subscriptioncmds.c:1959-1994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1959-L1994)

## Overview
AlterSubscriptionOwner changes the owner of a subscription identified by name, serving as the public interface for subscription ownership changes.

## Definition

```c
ObjectAddress
AlterSubscriptionOwner(const char *name, Oid newOwnerId)
```
## Detailed Description
AlterSubscriptionOwner is the public interface function for changing subscription ownership via subscription name. It acts as a wrapper around the internal implementation, handling catalog lookup and resource management.

The function performs the following operations:
1. Opens the pg_subscription catalog with exclusive row lock
2. Looks up the subscription by name in the system cache
3. Validates that the subscription exists, reporting an error if not found
4. Delegates the actual ownership change logic to AlterSubscriptionOwner_internal
5. Constructs and returns an ObjectAddress for the modified subscription
6. Cleans up allocated memory and closes the catalog relation

This function is typically called from the ALTER SUBSCRIPTION OWNER TO SQL command processing path.

## Parameters / Member Variables
- : String name of the subscription whose ownership should be changed
- : OID of the new owner role that will own the subscription

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy2: Looks up subscription by name in system cache
  - [AlterSubscriptionOwner_internal](AlterSubscriptionOwner_internal.md): Performs the actual ownership change logic
  - ObjectAddressSet: Constructs return address for the modified object
  - [heap_freetuple](../h/heap_freetuple.md): Frees memory allocated for the heap tuple
- Called from (representative examples):
  - [ExecAlterOwnerStmt](../E/ExecAlterOwnerStmt.md): Generic ALTER OWNER statement processor in alter.c:860

## Notes and Other Information
- Public interface function declared in subscriptioncmds.h header
- Uses RowExclusiveLock to prevent concurrent modifications during ownership change
- Returns ObjectAddress to support dependency tracking and event trigger integration
- Provides standard PostgreSQL error reporting for non-existent subscriptions
- Memory management includes proper cleanup of copied heap tuple
- Part of the standard PostgreSQL object ownership change infrastructure