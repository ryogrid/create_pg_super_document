# StopGeneratingPinnedObjectIds

## Location
[src/backend/access/transam/varsup.c:652-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L652-L672)

## Overview
Forces the OID counter to advance to FirstUnpinnedObjectId during initdb, ensuring that subsequent object creation produces unpinned objects rather than pinned ones.

## Definition
void StopGeneratingPinnedObjectIds(void)

## Detailed Description
This function is called once during database initialization (initdb) to transition from creating pinned objects to unpinned objects. During the early phases of initdb, the system needs to create some essential objects that should be "pinned" - meaning they have fixed, predictable OIDs that are reserved for system use. After these critical system objects are created, this function is called to advance the OID counter to FirstUnpinnedObjectId, ensuring that all subsequent objects created during the remainder of initdb will have unpinned (non-reserved) OIDs.

The function serves as a clear demarcation point in the initialization process, separating the creation of core system objects from user-level or variable system objects. This design ensures system stability by guaranteeing that essential PostgreSQL objects always have the same OIDs across installations.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - SetNextObjectId
  - FirstUnpinnedObjectId
- Called from (representative examples):
  - pg_stop_making_pinned_objects

## Notes and Other Information
- This function is specifically designed for use during database initialization and should not be called during normal database operation
- The transition point (FirstUnpinnedObjectId) is a compile-time constant that defines the boundary between reserved system OIDs and general-purpose OIDs
- This mechanism ensures consistent system catalog OID assignments across different PostgreSQL installations
- The function is called via the pg_stop_making_pinned_objects() SQL function, which provides the interface for initdb scripts