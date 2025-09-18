# type_in_list_does_not_exist_skipping

## Location
src/backend/commands/dropcmds.c: 206 - 242

## Overview
type_in_list_does_not_exist_skipping is a helper function that determines whether a missing function, cast, aggregate, or operator should be skipped because one of its involved datatypes or their schemas doesn't exist.

## Definition


## Detailed Description
This function is used when a specification for objects that involve datatypes (functions, casts, aggregates, operators) returns that the object doesn't exist. It iterates through a list of TypeNames to check if any of the involved datatypes or their schemas are missing. If a datatype or its schema doesn't exist, it sets appropriate error message and name parameters and returns true (indicating the missing object should be skipped). If all datatypes exist, it returns false, meaning the original object itself is genuinely missing.

## Parameters / Member Variables
- : List of TypeName objects representing the datatypes involved in the object specification
- : Output parameter for error message format string when skipping
- : Output parameter for the missing type or schema name to use in the error message

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeNameOid](../L/LookupTypeNameOid.md): Looks up type by name, returns InvalidOid if not found
  - [schema_does_not_exist_skipping](../s/schema_does_not_exist_skipping.md): Checks if the type's schema exists
  - [TypeNameToString](../T/TypeNameToString.md): Converts TypeName to string for error messages

- Called from (representative examples):
  - [does_not_exist_skipping](../d/does_not_exist_skipping.md): Used for function, cast, aggregate, and operator object types

## Notes and Other Information
- This is a static function internal to dropcmds.c
- Handles objects that depend on multiple datatypes (e.g., functions with parameters, operators with operands)
- Uses hierarchical checking: first checks schema existence, then type existence
- Part of the missing_ok logic for graceful handling of dependent objects
- Stops at the first missing type/schema found and reports that specific issue
- Helps distinguish between missing objects vs. missing types they depend on