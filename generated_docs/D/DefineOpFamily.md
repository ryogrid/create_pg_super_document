# DefineOpFamily

## Location
src/backend/commands/opclasscmds.c: 772 - 816

## Overview
Creates a new index operator family, which is a higher-level grouping that can contain multiple related operator classes for the same access method.

## Definition


## Detailed Description
DefineOpFamily implements the CREATE OPERATOR FAMILY SQL command. It creates a new operator family that can serve as a container for related operator classes. An operator family represents a collection of operators and support functions that are semantically compatible and can work together in index operations. This function performs basic validation and permission checks, then delegates the actual catalog insertion to CreateOpFamily.

The function is relatively simple compared to DefineOpClass because operator families are initially created empty - operators and functions are added later either through CREATE OPERATOR CLASS commands that reference the family, or through ALTER OPERATOR FAMILY commands.

## Parameters / Member Variables
- : Parsed CREATE OPERATOR FAMILY statement containing:
  - : List of names forming the operator family name
  - : Access method name for which this family is created

## Dependencies
- Functions called/Symbols referenced:
  - QualifiedNameGetCreationNamespace
  - object_aclcheck
  - aclcheck_error
  - get_namespace_name
  - get_index_am_oid
  - superuser
  - CreateOpFamily
- Called from (representative examples):
  - ProcessUtilitySlow (utility command processing)

## Notes and Other Information
- Requires superuser privileges for the same security reasons as DefineOpClass
- The function is essentially a wrapper that performs validation and then calls CreateOpFamily
- Operator families are created empty and populated later through other commands
- Access method validation ensures the specified access method exists and supports indexing
- Namespace permission checks ensure the user can create objects in the target schema
- Unlike operator classes, operator families don't have a default data type - they can contain operator classes for different types