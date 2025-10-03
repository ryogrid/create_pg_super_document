# findDependentObjects

## Location
[src/backend/catalog/dependency.c:432-979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L432-L979)

## Overview
Core recursive function that builds a complete dependency tree for object deletion, ensuring safe deletion order and handling complex dependency relationships including ownership, extensions, and partitioning.

## Definition

```c
static void
findDependentObjects(const ObjectAddress *object,
					 int objflags,
					 int flags,
					 ObjectAddressStack *stack,
					 ObjectAddresses *targetObjects,
					 const ObjectAddresses *pendingObjects,
					 Relation *depRel)
```
## Detailed Description
findDependentObjects is the heart of PostgreSQL's dependency analysis system. This complex recursive function performs comprehensive dependency traversal to determine all objects that must be deleted when dropping a given object. The function operates in several distinct phases:

**Phase 1 - Cycle Detection and Early Exits**: Checks for circular dependencies using a recursion stack, handles already-processed objects, and validates that pinned system objects cannot be dropped.

**Phase 2 - Ownership Analysis**: Scans pg_depend to identify objects that the current object depends on, particularly focusing on INTERNAL and EXTENSION dependencies that indicate ownership relationships. If the object is owned by another object, the function redirects to delete the owner first.

**Phase 3 - Dependent Object Collection**: Performs a reverse scan of pg_depend to find all objects that depend on the current object. These dependent objects are collected, sorted for consistent output, and then recursively processed.

**Phase 4 - Target Addition**: Finally adds the current object to the targetObjects list with appropriate flags and dependency metadata.

The function handles various dependency types (NORMAL, AUTO, INTERNAL, EXTENSION, PARTITION) with different behaviors for each. It ensures deletion order safety by processing dependencies before their dependents and includes sophisticated locking mechanisms to handle concurrent operations.

## Parameters / Member Variables
- `*object`: Pointer to ObjectAddress identifying the object to analyze for dependencies
- `objflags`: Integer flags describing the reason for visiting this object (DEPFLAG_ORIGINAL, DEPFLAG_NORMAL, etc.)
- `flags`: Integer bitmask of PERFORM_DELETION_* flags controlling overall deletion behavior
- `*stack`: Pointer to ObjectAddressStack for tracking recursion levels and detecting circular dependencies
- `*targetObjects`: Pointer to ObjectAddresses list where objects scheduled for deletion are accumulated
- `*pendingObjects`: Pointer to const ObjectAddresses list of other objects being deleted (used in multiple deletions)
- `*depRel`: Pointer to already-opened pg_depend relation for dependency queries
## Dependencies
- Functions called/Symbols referenced:
  - [stack_address_present_add_flags](../s/stack_address_present_add_flags.md)
  - [check_stack_depth](../c/check_stack_depth.md)
  - [object_address_present_add_flags](../o/object_address_present_add_flags.md)
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext/systable_endscan
  - [systable_recheck_tuple](../s/systable_recheck_tuple.md)
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md)/ReleaseDeletionLock
  - [object_address_present](../o/object_address_present.md)
  - qsort with object_address_comparator
  - [add_exact_object_address_extra](../a/add_exact_object_address_extra.md)
- Data structures used:
  - [ObjectAddress](../O/ObjectAddress.md)/ObjectAddresses
  - [ObjectAddressStack](../O/ObjectAddressStack.md)/ObjectAddressAndFlags
  - ObjectAddressExtra
  - Form_pg_depend
  - Various DEPENDENCY_* and DEPFLAG_* constants
- Called from (representative examples):
  - [performDeletion](../p/performDeletion.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [findDependentObjects](findDependentObjects.md) (recursive)
  - find_expr_references_context

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- The function is inherently recursive and includes stack depth checking to prevent overflow
- Uses sophisticated locking protocols to handle concurrent object deletions safely
- Dependency types have specific semantics: INTERNAL/EXTENSION require owner-first deletion, NORMAL/AUTO allow cascading deletion
- The function can redirect deletion requests when ownership dependencies are encountered
- Partition dependencies (PARTITION_PRI/PARTITION_SEC) are handled specially for partitioned tables
- Object flags accumulate through recursion to track the path by which each object was reached
- The sorting of dependent objects ensures predictable deletion order for consistent behavior
- Early exit mechanisms optimize performance by avoiding duplicate work on already-processed objects
- The pendingObjects parameter enables batch deletion optimizations in performMultipleDeletions

## Simplified Source

```c
static void findDependentObjects(const ObjectAddress *object, int objflags, int flags,
                                ObjectAddressStack *stack, ObjectAddresses *targetObjects,
                                const ObjectAddresses *pendingObjects, Relation *depRel) {

    // 1. Check for cycles and early exits
    if (stack_address_present_add_flags(object, objflags, stack))
        return;  // Already being processed

    if (object_address_present_add_flags(object, objflags, targetObjects))
        return;  // Already in target list

    if (IsPinnedObject(object->classId, object->objectId))
        ereport(ERROR, "cannot drop system object");

    // 2. Check if this object is owned by another object
    ObjectAddress owningObject = {0};
    ObjectAddress partitionObject = {0};

    // Scan dependencies from this object
    SysScanDesc scan = systable_beginscan(*depRel, DependDependerIndexId, true,
                                         NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);
        ObjectAddress otherObject = {foundDep->refclassid, foundDep->refobjid, foundDep->refobjsubid};

        switch (foundDep->deptype) {
            case DEPENDENCY_INTERNAL:
            case DEPENDENCY_EXTENSION:
                // This object is owned by another - redirect deletion there
                if (stack == NULL) {
                    owningObject = otherObject;
                    break;
                }
                if (stack_address_present_add_flags(&otherObject, 0, stack))
                    break;

                // Transfer deletion to owning object
                ReleaseDeletionLock(object);
                AcquireDeletionLock(&otherObject, 0);
                systable_endscan(scan);
                findDependentObjects(&otherObject, DEPFLAG_REVERSE, flags,
                                   stack, targetObjects, pendingObjects, depRel);
                return;

            case DEPENDENCY_PARTITION_PRI:
            case DEPENDENCY_PARTITION_SEC:
                objflags |= DEPFLAG_IS_PART;
                partitionObject = otherObject;
                break;
        }
    }
    systable_endscan(scan);

    // Error if we found an owning object at top level
    if (OidIsValid(owningObject.classId)) {
        ereport(ERROR, "cannot drop %s because %s requires it",
                getObjectDescription(object, false),
                getObjectDescription(&owningObject, false));
    }

    // 3. Find objects that depend on this object
    ObjectAddressAndFlags *dependentObjects;
    int numDependentObjects = 0;
    int maxDependentObjects = 128;
    dependentObjects = palloc(maxDependentObjects * sizeof(ObjectAddressAndFlags));

    // Scan for objects that reference this object
    scan = systable_beginscan(*depRel, DependReferenceIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);
        ObjectAddress otherObject = {foundDep->classid, foundDep->objid, foundDep->objsubid};

        // Skip self-references
        if (otherObject.classId == object->classId &&
            otherObject.objectId == object->objectId && object->objectSubId == 0)
            continue;

        // Lock dependent object
        AcquireDeletionLock(&otherObject, 0);

        // Determine flags based on dependency type
        int subflags;
        switch (foundDep->deptype) {
            case DEPENDENCY_NORMAL: subflags = DEPFLAG_NORMAL; break;
            case DEPENDENCY_AUTO: subflags = DEPFLAG_AUTO; break;
            case DEPENDENCY_INTERNAL: subflags = DEPFLAG_INTERNAL; break;
            case DEPENDENCY_PARTITION_PRI:
            case DEPENDENCY_PARTITION_SEC: subflags = DEPFLAG_PARTITION; break;
            case DEPENDENCY_EXTENSION: subflags = DEPFLAG_EXTENSION; break;
        }

        // Add to dependent objects list
        if (numDependentObjects >= maxDependentObjects) {
            maxDependentObjects *= 2;
            dependentObjects = repalloc(dependentObjects,
                                      maxDependentObjects * sizeof(ObjectAddressAndFlags));
        }
        dependentObjects[numDependentObjects].obj = otherObject;
        dependentObjects[numDependentObjects].subflags = subflags;
        numDependentObjects++;
    }
    systable_endscan(scan);

    // Sort dependent objects for consistent order
    if (numDependentObjects > 1)
        qsort(dependentObjects, numDependentObjects,
              sizeof(ObjectAddressAndFlags), object_address_comparator);

    // 4. Recursively process dependent objects
    ObjectAddressStack mystack = {object, objflags, stack};
    for (int i = 0; i < numDependentObjects; i++) {
        findDependentObjects(&dependentObjects[i].obj, dependentObjects[i].subflags,
                           flags, &mystack, targetObjects, pendingObjects, depRel);
    }

    pfree(dependentObjects);

    // 5. Finally add this object to the target list
    ObjectAddressExtra extra;
    extra.flags = mystack.flags;
    if (extra.flags & DEPFLAG_IS_PART)
        extra.dependee = partitionObject;
    else if (stack)
        extra.dependee = *stack->object;
    else
        memset(&extra.dependee, 0, sizeof(extra.dependee));

    add_exact_object_address_extra(object, &extra, targetObjects);
}
```