# reportDependentObjects

## Location
src/backend/catalog/dependency.c: 980 - 1035

## Overview
Validates deletion operations, reports cascading deletions to users, and enforces RESTRICT vs CASCADE behavior for dependency-based object deletion.

## Definition


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
  - getObjectDescription
  - message_level_is_interesting
  - initStringInfo
  - appendStringInfo/appendStringInfoChar
  - ereport/errmsg/errdetail/errhint
  - errmsg_plural/errmsg_internal
  - ngettext
  - pfree
- Data structures used:
  - ObjectAddresses/ObjectAddress
  - ObjectAddressExtra
  - StringInfoData
  - DropBehavior
  - Various DEPFLAG_* constants (DEPFLAG_ORIGINAL, DEPFLAG_IS_PART, etc.)
- Called from (representative examples):
  - performDeletion
  - performMultipleDeletions
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