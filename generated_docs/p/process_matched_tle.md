# process_matched_tle

## Location
src/backend/rewrite/rewriteHandler.c: 1036 - 1188

## Overview
Converts a matched TargetEntry from the original target list into a correct new TargetEntry, specifically handling multiple assignments to the same target attribute.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's rewrite system that handles complex UPDATE operations involving multiple assignments to the same column attribute. It intelligently combines FieldStore and SubscriptingRef operations when multiple assignments target the same attribute (e.g., ).

The function implements sophisticated logic to:
- Detect and validate multiple assignments to the same attribute
- Handle nested FieldStore and SubscriptingRef operations
- Manage CoerceToDomain nodes for domain-typed columns
- Combine multiple FieldStore operations into a single operation when possible
- Preserve assignment order (left-to-right execution)

For domain-typed columns, it strips CoerceToDomain nodes during processing and reconstitutes a single CoerceToDomain over the combined operations, ensuring domain checks are applied only after all field/element updates are complete.

## Parameters / Member Variables
- : The current TargetEntry being processed from the source target list
- : Previously processed TargetEntry for the same attribute (NULL if this is the first assignment)
- : The name of the target attribute (used only for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - get_assignment_input
  - CoerceToDomain
  - FieldStore
  - SubscriptingRef
  - equal
  - list_concat_copy
  - flatCopyTargetEntry
- Called from (representative examples):
  - rewriteTargetListIU

## Notes and Other Information
- Only allows multiple assignments if all are FieldStore or SubscriptingRef operations
- For FieldStore operations, combines multiple targets into a single FieldStore when possible
- For SubscriptingRef operations, nesting is always required
- Domain constraint checking is deferred until after all field updates are complete
- Generates syntax errors for incompatible multiple assignments to prevent data corruption