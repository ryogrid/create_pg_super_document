# AlterPublicationOwner_oid

## Location
[src/backend/commands/publicationcmds.c:1981-2000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1981-L2000)

## Overview
Changes the ownership of a PostgreSQL publication identified by OID, providing an alternative interface for publication ownership changes when the OID is already known.

## Definition
```c
void AlterPublicationOwner_oid(Oid subid, Oid newOwnerId)
```

## Detailed Description
This function serves as an OID-based interface for changing publication ownership, complementing AlterPublicationOwner which works with publication names. It handles the complete workflow of publication lookup by OID, validation, and ownership transfer. The function opens the pg_publication system catalog, searches for the publication by OID, and delegates the actual ownership change logic to AlterPublicationOwner_internal. It properly manages catalog access with appropriate locking and ensures memory cleanup.

This function is particularly useful in scenarios where the publication OID is already available, such as during dependency management operations or when processing system catalog maintenance tasks.

## Parameters / Member Variables
- `subid`: OID of the publication whose ownership will be changed
- `newOwnerId`: OID of the user who will become the new owner of the publication

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [AlterPublicationOwner_internal](AlterPublicationOwner_internal.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md)

## Notes and Other Information
- This is a public function, accessible throughout the PostgreSQL codebase and declared in publicationcmds.h
- Uses RowExclusiveLock on the pg_publication catalog to prevent concurrent modifications during ownership change
- Searches publications by OID using the PUBLICATIONOID system cache
- Provides a clear error message when the specified publication OID does not exist
- Does not return an ObjectAddress unlike AlterPublicationOwner, as the OID is already known by the caller
- Properly manages memory by freeing the heap tuple after processing
- Serves as a wrapper around AlterPublicationOwner_internal, handling the OID-based publication lookup and resource management
- Used primarily by internal PostgreSQL subsystems that work with object OIDs, such as the shared dependency management system
- More efficient than name-based lookup when the OID is already available