# get_required_extension

## Location
src/backend/commands/extension.c: 1697 - 1767

## Overview
get_required_extension retrieves the OID of a required extension, optionally installing it automatically if CASCADE mode is enabled and the extension is not yet installed.

## Definition


## Detailed Description
This function handles the resolution of extension dependencies during extension installation. It first attempts to find the required extension by name using get_extension_oid(). If the extension doesn't exist and CASCADE mode is enabled, it automatically installs the required extension by calling CreateExtensionInternal() recursively. The function implements important safety measures including cyclic dependency detection by checking the parents list, and provides helpful error messages with hints when required extensions are missing. It propagates the SCHEMA and CASCADE options to dependent extensions while maintaining proper parent tracking for cycle detection.

## Parameters / Member Variables
- : Name of the required extension to find or install
- : Name of the extension that requires this dependency (for error reporting)
- : Original schema name to propagate to dependent extensions
- : Whether to automatically install missing required extensions
- : List of extension names in current installation chain (for cycle detection)
- : Flag indicating if this is a CREATE operation (affects error hint messages)

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_oid](get_extension_oid.md)
  - [check_valid_extension_name](../c/check_valid_extension_name.md)
  - [list_copy](../l/list_copy.md)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md)

## Notes and Other Information
- This is a static function internal to extension.c that plays a crucial role in dependency management
- Implements cyclic dependency detection by scanning the parents list for the required extension name
- Provides user-friendly NOTICE messages when automatically installing required extensions
- Returns proper error codes (ERRCODE_INVALID_RECURSION, ERRCODE_UNDEFINED_OBJECT) with helpful hints
- Only propagates SCHEMA and CASCADE options to dependent extensions, not other CREATE EXTENSION options
- The parents list is extended with the current extension name before recursive calls to track the installation chain