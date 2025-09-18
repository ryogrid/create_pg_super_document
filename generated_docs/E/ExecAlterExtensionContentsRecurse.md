# ExecAlterExtensionContentsRecurse

## Location
src/backend/commands/extension.c: 3378 - 3516

## Overview
Performs the core logic of adding or removing objects from extensions, including handling dependent objects recursively without permission checks.

## Definition


## Detailed Description
This static function implements the actual membership modification logic for ALTER EXTENSION ADD/DROP operations. It operates recursively to handle dependent objects automatically, ensuring that related objects (like array types for base types, multirange types for range types, and row types for tables) maintain consistent extension membership.

For ADD operations, the function:
1. Verifies the object isn't already a member of another extension
2. Prevents circular dependencies (e.g., adding a schema that contains the extension)
3. Records the extension dependency relationship
4. Captures initial ACL states for the object

For DROP operations, the function:
1. Verifies the object is actually a member of the specified extension
2. Removes the dependency relationship
3. Cleans up extension configuration entries for relations
4. Removes stored initial ACL information

The recursive behavior ensures that type hierarchies and table relationships maintain consistency across extension membership changes.

## Parameters / Member Variables
- : AlterExtensionContentsStmt containing the operation details including action type (ADD/DROP > 0), extension name, and object specification
- : ObjectAddress representing the target extension
- : ObjectAddress representing the object being added to or removed from the extension

## Dependencies
- Functions called/Symbols referenced:
  - getExtensionOfObject
  - getObjectDescription
  - get_extension_name
  - get_extension_schema
  - get_namespace_name
  - recordDependencyOn
  - recordExtObjInitPriv
  - deleteDependencyRecordsForClass
  - extension_config_remove
  - removeExtObjInitPriv
  - get_array_type
  - type_is_range
  - get_range_multirange
  - get_rel_type_id
- Called from (representative examples):
  - ExecAlterExtensionContentsStmt (main entry point)
  - ExecAlterExtensionContentsRecurse (recursive calls for dependent objects)

## Notes and Other Information
- This is a static internal function within extension.c
- Handles both ADD (action > 0) and DROP (action <= 0) operations based on stmt->action
- Automatically recurses for dependent objects including array types, multirange types, and table row types
- Uses DEPENDENCY_EXTENSION for the dependency relationship type
- Manages initial privilege tracking through recordExtObjInitPriv/removeExtObjInitPriv
- Prevents circular dependencies by checking if a schema being added contains the extension itself
- For relations, also handles cleanup of extension configuration arrays (extconfig)