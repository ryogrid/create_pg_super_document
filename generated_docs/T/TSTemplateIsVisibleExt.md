# TSTemplateIsVisibleExt

## Location
src/backend/catalog/namespace.c: 3077 - 3151

## Overview
TSTemplateIsVisibleExt determines whether a text search template is visible in the current search path, with optional error handling for missing templates.

## Definition


## Detailed Description
This function performs a comprehensive visibility check for text search templates identified by their OID. It extends the basic visibility functionality by providing graceful error handling when a template is not found in the system catalog. The function implements PostgreSQL's standard namespace resolution rules to determine whether a template would be found during an unqualified name search.

The visibility determination process includes:
1. Looking up the template in the pg_ts_template system catalog
2. Checking if the template's namespace is in the current search path
3. Verifying that no template with the same name exists in an earlier namespace in the search path
4. Handling missing templates gracefully based on the is_missing parameter

The function follows PostgreSQL's namespace precedence rules where objects in earlier namespaces shadow those with the same name in later namespaces.

## Parameters / Member Variables
- : The OID of the text search template to check for visibility
- : Optional pointer to a boolean flag; if provided and the template is not found, this will be set to true instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (to look up template in pg_ts_template)
  - Form_pg_ts_template (struct type for template catalog entries)
  - recomputeNamespacePath (to ensure current search path)
  - list_member_oid (to check if namespace is in search path)
  - SearchSysCacheExists2 (to check for name conflicts)
  - ReleaseSysCache (to clean up cache reference)
- Called from (representative examples):
  - TSTemplateIsVisible (wrapper function)
  - pg_ts_template_is_visible (SQL-callable function)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Provides graceful error handling through the is_missing parameter, allowing callers to handle missing templates without exceptions
- Implements PostgreSQL's standard namespace visibility rules for text search objects
- Temporary namespaces are explicitly skipped during visibility checking
- The function follows the pattern of other *IsVisibleExt functions in the codebase for consistent error handling
- Essential for text search template management and proper namespace isolation