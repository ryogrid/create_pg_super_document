# deleteObjectsInList

## Location
[src/backend/catalog/dependency.c:185-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L185-L272)

## Overview
Executes the final deletion of objects in a dependency list, handling event triggers and performing actual object deletion in proper order.

## Definition

```c
struct a list of objects to delete (ie, the given object plus
	 * everything directly or indirectly dependent on it).
	 */
	targetObjects = new_object_addresses();
```
## Detailed Description
The deleteObjectsInList function is responsible for the final phase of object deletion in PostgreSQL's dependency management system. It operates on a list of objects that have already been determined to be safe for deletion and performs the actual deletion operations. The function handles two main responsibilities:

1. **Event Trigger Processing**: If event triggers are enabled and the deletion is not internal, it tracks dropped objects by calling EventTriggerSQLDropAddObject for each supported object type.

2. **Object Deletion**: Iterates through all target objects and calls deleteOneObject for each one, respecting flags that may skip original objects if specified.

The function processes objects in the order they appear in the targetObjects list, which should already be properly sorted by the dependency analysis phase to ensure safe deletion order (dependencies deleted before their dependents).

## Parameters / Member Variables
- : Pointer to ObjectAddresses structure containing the list of objects to be deleted along with their metadata
- : Pointer to Relation structure representing the dependency relationship context
- : Integer bitmask controlling deletion behavior, including PERFORM_DELETION_INTERNAL and PERFORM_DELETION_SKIP_ORIGINAL

## Dependencies
- Functions called/Symbols referenced:
  - [trackDroppedObjectsNeeded](../t/trackDroppedObjectsNeeded.md)
  - [EventTriggerSupportsObject](../E/EventTriggerSupportsObject.md)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md)
  - [deleteOneObject](deleteOneObject.md)
- Data structures used:
  - ObjectAddresses
  - ObjectAddressExtra
  - DEPFLAG_ORIGINAL
  - DEPFLAG_NORMAL
  - DEPFLAG_REVERSE
- Called from (representative examples):
  - [performDeletion](../p/performDeletion.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- The function assumes that dependency analysis has already been performed and objects are in safe deletion order
- Event trigger support is conditional and only applies to non-internal deletions
- The PERFORM_DELETION_SKIP_ORIGINAL flag allows callers to delete dependencies without deleting the original requested objects
- Object flags (DEPFLAG_*) are used to categorize objects as original, normal, or reverse dependencies for event trigger purposes

## Simplified Source

```c
static void deleteObjectsInList(ObjectAddresses *targetObjects, Relation *depRel,
                               int flags) {
    // Track dropped objects for event triggers if needed
    if (trackDroppedObjectsNeeded() && !(flags & PERFORM_DELETION_INTERNAL)) {
        for (int i = 0; i < targetObjects->numrefs; i++) {
            const ObjectAddress *obj = &targetObjects->refs[i];
            const ObjectAddressExtra *extra = &targetObjects->extras[i];

            // Determine object type flags for event triggers
            bool original = (extra->flags & DEPFLAG_ORIGINAL) != 0;
            bool normal = (extra->flags & DEPFLAG_NORMAL) != 0 ||
                         (extra->flags & DEPFLAG_REVERSE) != 0;

            // Add to event trigger tracking if supported
            if (EventTriggerSupportsObject(obj)) {
                EventTriggerSQLDropAddObject(obj, original, normal);
            }
        }
    }

    // Delete all objects in proper order
    for (int i = 0; i < targetObjects->numrefs; i++) {
        ObjectAddress *obj = &targetObjects->refs[i];
        ObjectAddressExtra *extra = &targetObjects->extras[i];

        // Skip original objects if requested
        if ((flags & PERFORM_DELETION_SKIP_ORIGINAL) &&
            (extra->flags & DEPFLAG_ORIGINAL)) {
            continue;
        }

        // Perform the actual deletion
        deleteOneObject(obj, depRel, flags);
    }
}
```