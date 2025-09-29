# reportDependentObjects

## Location
[src/backend/catalog/dependency.c:980-1035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L980-L1035)

## Overview
Validates deletion operations, reports cascading deletions to users, and enforces RESTRICT vs CASCADE behavior for dependency-based object deletion.

## Definition

```c
static void
reportDependentObjects(const ObjectAddresses *targetObjects,
					   DropBehavior behavior,
					   int flags,
					   const ObjectAddress *origObject)
```
## Detailed Description
reportDependentObjects serves as both a validation and reporting function in PostgreSQL's dependency deletion system. It performs several critical functions:

**Validation Phase**: First validates that partition-dependent objects have proper partition dependencies, preventing inconsistent deletion states where a partition object would be deleted without its partition parent.

**Reporting Phase**: Provides user feedback about what objects will be deleted in CASCADE mode or what dependencies prevent deletion in RESTRICT mode. The function distinguishes between different types of dependencies and reports them appropriately:

- **Auto-cascades**: Objects deleted due to AUTO, INTERNAL, PARTITION, or EXTENSION dependencies are reported at DEBUG2 level as "auto-cascades"
- **Normal cascades**: Objects deleted due to NORMAL dependencies are reported as "drop cascades to..." messages
- **RESTRICT violations**: When behavior is DROP_RESTRICT, dependent objects are reported as errors with "depends on" messages

**Error Enforcement**: In RESTRICT mode, if any normal dependencies exist, the function throws an error preventing the deletion. In CASCADE mode, it simply reports what will be cascaded.

**Message Management**: Implements smart message handling with client-side message limiting (MAX_REPORTED_DEPS = 100) while logging complete details to the server log. Uses different message levels based on the PERFORM_DELETION_QUIETLY flag.

## Parameters / Member Variables
- : Pointer to ObjectAddresses containing all objects scheduled for deletion with their dependency metadata
- : DropBehavior enum (DROP_RESTRICT or DROP_CASCADE) controlling how dependencies are handled  
- : Integer bitmask including PERFORM_DELETION_QUIETLY to control message verbosity level
- : Pointer to ObjectAddress of the original object being dropped (NULL for DROP OWNED operations)

## Dependencies
- Functions called/Symbols referenced:
  - [getObjectDescription](../g/getObjectDescription.md)
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)/appendStringInfoChar
  - ereport/errmsg/errdetail/errhint
  - [errmsg_plural](../e/errmsg_plural.md)/errmsg_internal
  - ngettext
  - [pfree](../p/pfree.md)
- Data structures used:
  - ObjectAddresses/ObjectAddress
  - ObjectAddressExtra
  - [StringInfoData](../S/StringInfoData.md)
  - DropBehavior
  - Various DEPFLAG_* constants (DEPFLAG_ORIGINAL, DEPFLAG_IS_PART, etc.)
- Called from (representative examples):
  - [performDeletion](../p/performDeletion.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - find_expr_references_context

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- The function processes objects in reverse order (dependency order) for more understandable user output
- Implements sophisticated message limiting to prevent overwhelming clients with massive dependency lists
- Different dependency types receive different treatment: auto-cascades are barely visible while normal cascades are prominently reported
- RESTRICT mode errors provide helpful hints suggesting CASCADE as an alternative
- The partition validation prevents subtle bugs in partitioned table management
- Original deletion targets are excluded from dependency reporting to avoid redundant messages
- Sub-objects are also excluded from reporting since the whole object is reported elsewhere
- Message localization is supported through the underscore macro _() for translatable strings
- Server and client logs may have different levels of detail based on configuration settings

## Simplified Source

```c
static void
reportDependentObjects(const ObjectAddresses *targetObjects,
                      DropBehavior behavior,
                      int flags,
                      const ObjectAddress *origObject)
{
    int msglevel = (flags & PERFORM_DELETION_QUIETLY) ? DEBUG2 : NOTICE;
    bool ok = true;
    StringInfoData clientdetail, logdetail;
    int numReportedClient = 0, numNotReportedClient = 0;

    // Validate partition dependencies - ensure partition objects have proper dependencies
    for (int i = 0; i < targetObjects->numrefs; i++)
    {
        const ObjectAddressExtra *extra = &targetObjects->extras[i];

        if ((extra->flags & DEPFLAG_IS_PART) && !(extra->flags & DEPFLAG_PARTITION))
        {
            // Error: trying to delete partition object without its partition dependency
            const ObjectAddress *object = &targetObjects->refs[i];
            char *otherObjDesc = getObjectDescription(&extra->dependee, false);

            ereport(ERROR,
                   (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop %s because %s requires it",
                           getObjectDescription(object, false), otherObjDesc),
                    errhint("You can drop %s instead.", otherObjDesc)));
        }
    }

    // Early return if CASCADE mode and message level too low
    if (behavior == DROP_CASCADE && !message_level_is_interesting(msglevel))
        return;

    // Initialize detail strings for client and server logging
    initStringInfo(&clientdetail);
    initStringInfo(&logdetail);

    // Process each object and categorize dependencies
    for (int i = 0; i < targetObjects->numrefs; i++)
    {
        const ObjectAddress *obj = &targetObjects->refs[i];
        const ObjectAddressExtra *extra = &targetObjects->extras[i];

        // Skip original objects and sub-objects
        if ((extra->flags & DEPFLAG_ORIGINAL) || (extra->flags & DEPFLAG_SUBOBJECT))
            continue;

        char *objDesc = getObjectDescription(obj, false);

        // Categorize dependency types for appropriate reporting
        if (extra->flags & (DEPFLAG_AUTO | DEPFLAG_INTERNAL | DEPFLAG_PARTITION | DEPFLAG_EXTENSION))
        {
            // Auto-cascades - minimal visibility reporting
            appendStringInfo(&logdetail, "drop auto-cascades to %s\n", objDesc);
        }
        else
        {
            // Normal cascades - prominent reporting
            if (numReportedClient < MAX_REPORTED_DEPS)
            {
                appendStringInfo(&clientdetail,
                    behavior == DROP_CASCADE ? "drop cascades to %s\n" : "%s depends on\n",
                    objDesc);
                numReportedClient++;
            }
            else
                numNotReportedClient++;

            appendStringInfo(&logdetail,
                behavior == DROP_CASCADE ? "drop cascades to %s\n" : "%s depends on\n",
                objDesc);
        }

        pfree(objDesc);
    }

    // Report to client with message limiting
    if (numReportedClient > 0)
    {
        if (behavior == DROP_CASCADE)
        {
            ereport(msglevel,
                   (errmsg_plural("drop cascades to %d other object",
                                  "drop cascades to %d other objects",
                                  numReportedClient + numNotReportedClient,
                                  numReportedClient + numNotReportedClient),
                    numReportedClient > 0 ? errdetail("%s", clientdetail.data) : 0));
        }
        else
        {
            // RESTRICT mode - report dependencies and error out
            ereport(ERROR,
                   (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg_plural("%d dependent object",
                                  "%d dependent objects",
                                  numReportedClient + numNotReportedClient,
                                  numReportedClient + numNotReportedClient),
                    errdetail("%s", clientdetail.data)));
            ok = false;
        }
    }

    // Always log complete details to server log
    if (logdetail.len > 0)
    {
        ereport(LOG, (errmsg_internal("%s", logdetail.data)));
    }

    pfree(clientdetail.data);
    pfree(logdetail.data);
}
```