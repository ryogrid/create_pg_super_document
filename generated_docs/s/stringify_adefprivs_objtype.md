# stringify_adefprivs_objtype

## Location
src/backend/commands/event_trigger.c: 2206 - 2282

## Overview
Converts PostgreSQL ObjectType enumeration values to their plural string representations as they appear in ALTER DEFAULT PRIVILEGES commands.

## Definition
```c
static const char *stringify_adefprivs_objtype(ObjectType objtype)
```

## Detailed Description
This static function serves as a specialized mapping utility that translates PostgreSQL's internal ObjectType enumeration values into the standardized plural string representations used in SQL ALTER DEFAULT PRIVILEGES statements. Unlike stringify_grant_objtype which returns singular forms, this function specifically returns plural forms as required by the ALTER DEFAULT PRIVILEGES syntax.

The function handles the same subset of object types that support privilege operations in PostgreSQL, but formats them as plurals (e.g., "TABLES" instead of "TABLE", "FUNCTIONS" instead of "FUNCTION"). It maintains the same comprehensive error handling for unsupported object types that cannot be used with ALTER DEFAULT PRIVILEGES commands.

## Parameters / Member Variables
- `objtype`: The ObjectType enumeration value to be converted to its plural string representation

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - ObjectType enumeration constants (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
- Called from (representative examples):
  - pg_event_trigger_ddl_commands (src/backend/commands/event_trigger.c:2075)

## Notes and Other Information
- Returns plural string constants that match the exact syntax used in SQL ALTER DEFAULT PRIVILEGES statements
- Includes special plural mappings for object types with multi-word names (e.g., "FOREIGN DATA WRAPPERS", "LARGE OBJECTS")
- Contains explicit error handling for unsupported object types, indicating that they cannot be used with ALTER DEFAULT PRIVILEGES
- The function distinguishes between FUNCTIONS, PROCEDURES, and ROUTINES object types in their plural forms
- Used primarily in event trigger contexts to provide human-readable object type information for default privilege-related DDL commands
- Complements stringify_grant_objtype by providing the alternate syntax format required for ALTER DEFAULT PRIVILEGES
- The extensive list of unsupported object types mirrors that of stringify_grant_objtype, maintaining consistency in PostgreSQL's privilege system limitations
- Essential for proper event trigger reporting of ALTER DEFAULT PRIVILEGES commands, ensuring the object type appears in the correct grammatical form expected by users and applications