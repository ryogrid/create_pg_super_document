# dumpComment

## Location
src/bin/pg_dump/pg_dump.c: 10246 - 10261

## Overview
A simplified wrapper function that calls dumpCommentExtended with no initdb comment handling, used for dumping standard object comments.

## Definition


## Detailed Description
This function serves as a streamlined interface to dumpCommentExtended for cases where no special initdb comment processing is needed. It simply passes all parameters through to dumpCommentExtended with NULL for the initdb_comment parameter. This is the most commonly used function for dumping comments on database objects throughout pg_dump.

## Parameters / Member Variables
- : Archive context for the dump operation
- : Object type string (e.g., "TABLE", "FUNCTION", "INDEX")
- : Object name ready for printing (without schema decoration)
- : Schema namespace of the object for labeling
- : Owner of the object for labeling
- : Catalog identifier (tableoid and oid) for pg_description lookup
- : Sub-object identifier for pg_description lookup (0 for main object)
- : Dump ID for dependency tracking in the output

## Dependencies
- Functions called/Symbols referenced:
  - [dumpCommentExtended](dumpCommentExtended.md)
- Called from (representative examples):
  - [dumpLO](dumpLO.md)
  - [dumpPolicy](dumpPolicy.md)
  - [dumpPublication](dumpPublication.md)
  - [dumpSubscription](dumpSubscription.md)
  - [dumpExtension](dumpExtension.md)
  - [dumpEnumType](dumpEnumType.md)
  - [dumpRangeType](dumpRangeType.md)
  - [dumpBaseType](dumpBaseType.md)
  - [dumpDomain](dumpDomain.md)
  - [dumpFunc](dumpFunc.md)
  - [dumpIndex](dumpIndex.md)
  - [dumpSequence](dumpSequence.md)
  - [dumpTrigger](dumpTrigger.md)
  - [dumpRule](dumpRule.md)

## Notes and Other Information
- This is the standard function used by most object dump routines for comment handling
- Marked as inline for performance optimization since it's just a simple wrapper
- Does not handle initdb comments, making it suitable for user-created objects
- Widely used throughout pg_dump for consistent comment dumping across different object types