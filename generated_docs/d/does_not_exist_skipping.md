# does_not_exist_skipping

## Location
src/backend/commands/dropcmds.c: 243 - 524

## Overview
does_not_exist_skipping generates appropriate NOTICE messages when objects specified in DROP IF EXISTS statements are not found, implementing intelligent error reporting that distinguishes between missing objects and missing dependencies.

## Definition


## Detailed Description
This function is the central dispatcher for generating NOTICE messages when objects don't exist in DROP IF EXISTS operations. It contains a large switch statement that handles different object types, using hierarchical checking through helper functions to determine whether the object itself is missing or its dependencies (schemas, types, owning relations) are missing. The function provides user-friendly error messages that accurately describe what is missing and being skipped.

## Parameters / Member Variables
- : ObjectType enum value indicating the type of object being dropped (OBJECT_FUNCTION, OBJECT_TYPE, etc.)
- : Node pointer containing the object specification (name, arguments, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - schema_does_not_exist_skipping: Checks if schema exists for schema-qualified objects
  - type_in_list_does_not_exist_skipping: Checks if types exist for objects with type dependencies
  - owningrel_does_not_exist_skipping: Checks if owning relation exists for triggers/rules/policies
  - TypeNameToString: Converts TypeName to string for messages
  - NameListToString: Converts name lists to strings for messages
  - TypeNameListToString: Converts type argument lists to strings
  - Various list manipulation functions (list_copy_head, list_copy_tail, etc.)

- Called from (representative examples):
  - RemoveObjects: Main caller when objects are not found during DROP operations

## Notes and Other Information
- This is a static function internal to dropcmds.c
- Only relevant when missing_ok is true (IF EXISTS clause)
- Handles 20+ different object types with appropriate error messages
- Uses hierarchical checking to provide precise error information
- Some object types (relations, roles, etc.) are handled elsewhere and trigger errors if passed here
- Supports objects with complex specifications (functions with arguments, casts between types)
- Always generates NOTICE level messages, never ERROR (since IF EXISTS allows missing objects)
- Messages are internationalized using gettext_noop