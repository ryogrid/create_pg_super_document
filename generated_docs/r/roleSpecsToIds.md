# roleSpecsToIds

## Location
src/backend/commands/user.c: 1652 - 1680

## Overview
Converts a list of RoleSpec structures to a corresponding list of role OIDs while maintaining the same order and validating role existence.

## Definition


## Detailed Description
roleSpecsToIds is a utility function that transforms a list of RoleSpec structures (which represent role specifications in various formats like names, OIDs, or special keywords) into a list of concrete role OIDs. The function preserves the original order of the input list and ensures that all specified roles exist in the system by using get_rolespec_oid with the 'missing_ok' parameter set to false.

This function serves as a critical conversion step in many role management operations, providing a standardized way to resolve role specifications into the OID format required by lower-level catalog functions. It rejects ROLESPEC_PUBLIC specifications as documented in the comment.

## Parameters / Member Variables
- : List of RoleSpec structures to be converted to OIDs

## Dependencies
- Functions called/Symbols referenced:
  - get_rolespec_oid: Convert individual RoleSpec to OID with existence validation
  - lappend_oid: Append OID to the result list
  - lfirst_node: Extract RoleSpec from list cell
- Called from (representative examples):
  - CreateRole: Role creation with membership specifications
  - AlterRole: Role modification with membership changes
  - GrantRole: Role grant operations
  - DropOwnedObjects: Object dropping by role ownership
  - ReassignOwnedObjects: Object ownership reassignment
  - AlterTableMoveAll: Table movement operations

## Notes and Other Information
- Maintains the same order as the input list for consistent behavior across operations
- ROLESPEC_PUBLIC is explicitly not allowed and will cause get_rolespec_oid to fail
- The function will error if any specified role does not exist (missing_ok = false)
- Widely used throughout the role management system as a standard conversion utility
- Returns a new List that should be freed by the caller when no longer needed
- Essential for bridging the gap between parser-level role specifications and catalog-level OID operations