# ViewOptions

## Location
src/include/utils/rel.h: 413 - 419

## Overview
ViewOptions is a structure that defines relation options (reloptions) specifically for views, containing security and constraint checking configuration parameters that control view behavior and access policies.

## Definition


## Detailed Description
ViewOptions serves as the specialized structure for storing relation options for views in PostgreSQL. This structure is stored in the rd_options field of view relation descriptors and contains parameters that control view security behavior and constraint checking policies. The structure follows the PostgreSQL varlena format, allowing it to be stored as variable-length data. These options affect how views handle security contexts, user permissions, and constraint validation, and can be configured through CREATE VIEW or ALTER VIEW statements with specific option clauses.

## Parameters / Member Variables
- : Varlena header for variable-length data structure (internal use, should not be modified directly)
- : Boolean flag indicating whether the view should be treated as a security barrier view, preventing optimization pushdown that might leak information
- : Boolean flag controlling whether the view executes with the permissions of the invoker (true) or the view owner (false)
- : Enum value controlling constraint checking behavior for INSERT/UPDATE operations through the view (NOT_SET, LOCAL, or CASCADED)

## Dependencies
- Functions called/Symbols referenced:
  - ViewOptCheckOption
- Called from (representative examples):
  - view_reloptions
  - RelationIsSecurityView
  - RelationHasSecurityInvoker
  - RelationHasCheckOption
  - RelationHasLocalCheckOption
  - RelationHasCascadedCheckOption

## Notes and Other Information
ViewOptions is specifically designed for view relations and provides security and constraint enforcement controls that are unique to views. The security_barrier option is crucial for row-level security implementations, as it prevents the query planner from pushing conditions below the view that might expose restricted data. The security_invoker option determines the security context for view execution, affecting which user's permissions are used when accessing underlying tables. The check_option setting controls how constraint violations are handled when modifying data through updatable views, with LOCAL checking only the view's constraints and CASCADED checking all underlying view constraints as well.