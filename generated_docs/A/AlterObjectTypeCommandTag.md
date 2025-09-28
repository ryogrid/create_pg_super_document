# AlterObjectTypeCommandTag

## Location
[src/backend/tcop/utility.c:2214-2359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L2214-L2359)

## Overview
AlterObjectTypeCommandTag is a static helper function that maps PostgreSQL object types to their corresponding ALTER command tags for logging and monitoring purposes.

## Definition

```c
static CommandTag
AlterObjectTypeCommandTag(ObjectType objtype)
```
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

## Simplified Source

```c
// Simplified version of AlterObjectTypeCommandTag
static CommandTag AlterObjectTypeCommandTag(ObjectType objtype) {
    CommandTag tag;

    // Map object types to their corresponding ALTER command tags
    switch (objtype) {
        case OBJECT_AGGREGATE:      tag = CMDTAG_ALTER_AGGREGATE; break;
        case OBJECT_ATTRIBUTE:      tag = CMDTAG_ALTER_TYPE; break;
        case OBJECT_CAST:           tag = CMDTAG_ALTER_CAST; break;
        case OBJECT_COLLATION:      tag = CMDTAG_ALTER_COLLATION; break;
        case OBJECT_COLUMN:         tag = CMDTAG_ALTER_TABLE; break;
        case OBJECT_CONVERSION:     tag = CMDTAG_ALTER_CONVERSION; break;
        case OBJECT_DATABASE:       tag = CMDTAG_ALTER_DATABASE; break;
        case OBJECT_DOMAIN:
        case OBJECT_DOMCONSTRAINT:  tag = CMDTAG_ALTER_DOMAIN; break;
        case OBJECT_EXTENSION:      tag = CMDTAG_ALTER_EXTENSION; break;
        case OBJECT_FDW:            tag = CMDTAG_ALTER_FOREIGN_DATA_WRAPPER; break;
        case OBJECT_FOREIGN_SERVER: tag = CMDTAG_ALTER_SERVER; break;
        case OBJECT_FOREIGN_TABLE:  tag = CMDTAG_ALTER_FOREIGN_TABLE; break;
        case OBJECT_FUNCTION:       tag = CMDTAG_ALTER_FUNCTION; break;
        case OBJECT_INDEX:          tag = CMDTAG_ALTER_INDEX; break;
        case OBJECT_LANGUAGE:       tag = CMDTAG_ALTER_LANGUAGE; break;
        case OBJECT_LARGEOBJECT:    tag = CMDTAG_ALTER_LARGE_OBJECT; break;
        case OBJECT_OPCLASS:        tag = CMDTAG_ALTER_OPERATOR_CLASS; break;
        case OBJECT_OPERATOR:       tag = CMDTAG_ALTER_OPERATOR; break;
        case OBJECT_OPFAMILY:       tag = CMDTAG_ALTER_OPERATOR_FAMILY; break;
        case OBJECT_POLICY:         tag = CMDTAG_ALTER_POLICY; break;
        case OBJECT_PROCEDURE:      tag = CMDTAG_ALTER_PROCEDURE; break;
        case OBJECT_ROLE:           tag = CMDTAG_ALTER_ROLE; break;
        case OBJECT_ROUTINE:        tag = CMDTAG_ALTER_ROUTINE; break;
        case OBJECT_RULE:           tag = CMDTAG_ALTER_RULE; break;
        case OBJECT_SCHEMA:         tag = CMDTAG_ALTER_SCHEMA; break;
        case OBJECT_SEQUENCE:       tag = CMDTAG_ALTER_SEQUENCE; break;
        case OBJECT_TABLE:
        case OBJECT_TABCONSTRAINT:  tag = CMDTAG_ALTER_TABLE; break;
        case OBJECT_TABLESPACE:     tag = CMDTAG_ALTER_TABLESPACE; break;
        case OBJECT_TRIGGER:        tag = CMDTAG_ALTER_TRIGGER; break;
        case OBJECT_EVENT_TRIGGER:  tag = CMDTAG_ALTER_EVENT_TRIGGER; break;
        case OBJECT_TSCONFIGURATION: tag = CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION; break;
        case OBJECT_TSDICTIONARY:   tag = CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY; break;
        case OBJECT_TSPARSER:       tag = CMDTAG_ALTER_TEXT_SEARCH_PARSER; break;
        case OBJECT_TSTEMPLATE:     tag = CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE; break;
        case OBJECT_TYPE:           tag = CMDTAG_ALTER_TYPE; break;
        case OBJECT_VIEW:           tag = CMDTAG_ALTER_VIEW; break;
        case OBJECT_MATVIEW:        tag = CMDTAG_ALTER_MATERIALIZED_VIEW; break;
        case OBJECT_PUBLICATION:    tag = CMDTAG_ALTER_PUBLICATION; break;
        case OBJECT_SUBSCRIPTION:   tag = CMDTAG_ALTER_SUBSCRIPTION; break;
        case OBJECT_STATISTIC_EXT:  tag = CMDTAG_ALTER_STATISTICS; break;
        default:                    tag = CMDTAG_UNKNOWN; break;
    }

    return tag;
}
```

Key simplifications made:
- Consolidated the switch statement into a more compact format
- Added clear comment about the function's purpose
- Maintained all object type mappings including special cases
- Preserved the fallback to CMDTAG_UNKNOWN for safety
- Aligned case statements for better readability
- Kept essential groupings (like OBJECT_DOMAIN/OBJECT_DOMCONSTRAINT)