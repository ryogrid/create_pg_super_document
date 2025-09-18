# TSTemplateIsVisible

## Location
src/backend/catalog/namespace.c: 3065 - 3076

## Overview
TSTemplateIsVisible determines whether a text search template is visible in the current search path by checking if it would be found by an unqualified name search.

## Definition


## Detailed Description
This function provides a simple interface for checking the visibility of a text search template identified by its OID. It acts as a wrapper around the more comprehensive TSTemplateIsVisibleExt function, providing the standard visibility check without the extended error handling capabilities. The function determines whether the specified template would be found when searching for its unqualified name through the current namespace search path, following PostgreSQL's standard name resolution rules.

The visibility check considers whether the template's namespace is in the current search path and whether it would be the first template with that name encountered during a search path traversal.

## Parameters / Member Variables
- : The OID of the text search template to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - TSTemplateIsVisibleExt (extended visibility check function)
- Called from (representative examples):
  - getObjectDescription (for object description generation)
  - Various namespace and catalog management functions

## Notes and Other Information
- This is a simple wrapper function that delegates to TSTemplateIsVisibleExt with NULL for the is_missing parameter
- Follows the standard PostgreSQL pattern of having both basic and extended versions of visibility check functions
- Will throw an error if the template OID is not found in the system catalog (since is_missing is NULL)
- Used primarily in contexts where template existence is already confirmed and only visibility needs to be checked
- Part of the broader text search infrastructure for managing template visibility and namespace resolution