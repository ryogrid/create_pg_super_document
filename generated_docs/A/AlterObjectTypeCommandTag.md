# AlterObjectTypeCommandTag

## Location
src/backend/tcop/utility.c: 2214 - 2359

## Overview
AlterObjectTypeCommandTag is a static helper function that maps PostgreSQL object types to their corresponding ALTER command tags for logging and monitoring purposes.

## Definition


## Detailed Description
This function serves as a centralized mapping utility within PostgreSQL's utility command processing system. It takes an ObjectType enumeration value and returns the appropriate CommandTag that represents the ALTER operation for that specific object type. The function covers most database objects that support ALTER operations, providing a systematic way to generate consistent command tags for logging, auditing, and command completion tracking.

The function uses a comprehensive switch statement to handle over 30 different object types, ensuring that each ALTER operation is properly categorized with its corresponding command tag. For unrecognized object types, it returns CMDTAG_UNKNOWN as a fallback.

## Parameters / Member Variables
- : An ObjectType enum value representing the type of database object being altered (e.g., OBJECT_TABLE, OBJECT_FUNCTION, OBJECT_INDEX, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType (enum parameter)
  - CommandTag (return type)
  - CMDTAG_ALTER_* constants (various ALTER command tags)
  - CMDTAG_UNKNOWN (fallback tag)
- Called from (representative examples):
  - [CreateCommandTag](../C/CreateCommandTag.md) (multiple call sites in utility.c:2677, 2683, 2687, 2691, 2695, 2699)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the utility.c file
- The function handles special cases where multiple object types map to the same command tag (e.g., OBJECT_DOMAIN and OBJECT_DOMCONSTRAINT both map to CMDTAG_ALTER_DOMAIN)
- Some object types like OBJECT_COLUMN and OBJECT_TABCONSTRAINT map to CMDTAG_ALTER_TABLE, reflecting their relationship to table operations
- The function provides comprehensive coverage of PostgreSQL's object hierarchy for ALTER operations
- Returns CMDTAG_UNKNOWN for any unrecognized object types, ensuring the function always returns a valid CommandTag