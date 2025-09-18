# CreateExtension

## Location
src/backend/commands/extension.c: 1768 - 1865

## Overview
CreateExtension is the main entry point function that implements the CREATE EXTENSION SQL command by parsing statement options and delegating the actual work to CreateExtensionInternal.

## Definition


## Detailed Description
This function serves as the primary interface for the CREATE EXTENSION SQL command. It validates the extension name, checks for duplicates (with support for IF NOT EXISTS), parses statement options (schema, new_version, cascade), and prevents nested extension creation. The function handles all the user-facing aspects of the command including option validation, duplicate detection, and proper error reporting before calling CreateExtensionInternal() to perform the actual installation work. It maintains global state to prevent concurrent extension creation operations.

## Parameters / Member Variables
- : Parser state for error reporting and context
- : Parsed CREATE EXTENSION statement containing extension name, options, and flags

## Dependencies
- Functions called/Symbols referenced:
  - [check_valid_extension_name](../c/check_valid_extension_name.md)
  - [get_extension_oid](../g/get_extension_oid.md)
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md)
  - [defGetString](../d/defGetString.md)
  - [defGetBoolean](../d/defGetBoolean.md)
  - [CreateExtensionInternal](CreateExtensionInternal.md)
- Types referenced:
  - CreateExtensionStmt
  - [DefElem](../D/DefElem.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- This is the public interface function declared in extension.h and called by the utility command processor
- Implements comprehensive validation including extension name validity and duplicate checking
- Supports the IF NOT EXISTS clause with appropriate NOTICE messages
- Uses global variables (creating_extension) to prevent nested CREATE EXTENSION operations
- Parses and validates three main options: schema, new_version, and cascade
- Always passes NIL as the parents list and true as the is_create flag to CreateExtensionInternal
- Provides proper error codes and user-friendly error messages for various failure scenarios