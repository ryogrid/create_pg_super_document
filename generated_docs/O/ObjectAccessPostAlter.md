# ObjectAccessPostAlter

## Location
[src/include/catalog/objectaccess.h:103-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/objectaccess.h#L103-L123)

## Overview
ObjectAccessPostAlter is a struct that holds arguments for the OAT_POST_ALTER object access hook event, providing context information about object alteration operations to security and logging extensions.

## Definition


## Detailed Description
The ObjectAccessPostAlter struct serves as a parameter container for object access hooks that are triggered after object alteration (OAT_POST_ALTER events). It provides essential context information to extensions about the nature and circumstances of the object modification operation.

This struct addresses two key aspects of object alteration context:
1. **Composite Object Identification**: Some system catalogs require two OIDs to uniquely identify a tuple, and the auxiliary_id field supports this for catalogs like pg_inherits, pg_db_role_setting, and pg_user_mapping.
2. **Internal vs. User Operations**: The is_internal flag distinguishes between user-requested alterations and internal PostgreSQL operations (such as constraint modifications during CLUSTER operations).

Extensions can use this information to implement appropriate security policies, skip unnecessary permission checks for internal operations, or apply different logging strategies based on the alteration context.

## Parameters / Member Variables
- : OID used for catalogs that require two IDs to identify a specific tuple (pg_inherits, pg_db_role_setting, pg_user_mapping). Should be set to InvalidOid for other catalogs.
- : Boolean flag indicating whether the alteration is internal to PostgreSQL operations (true) rather than explicitly requested by the user (false). Permissions-checking hooks may skip checks for internal operations.

## Dependencies
- Functions called/Symbols referenced:
  - InvalidOid (for auxiliary_id initialization)
- Called from (representative examples):
  - [RunObjectPostAlterHook](../R/RunObjectPostAlterHook.md)
  - [RunObjectPostAlterHookStr](../R/RunObjectPostAlterHookStr.md)
  - [accesstype_arg_to_string](../a/accesstype_arg_to_string.md)

## Notes and Other Information
- This struct is specifically used with OAT_POST_ALTER hook events
- The auxiliary_id field is only meaningful for specific system catalogs that use composite keys
- Extensions should be cautious about applying full permission checks when is_internal is true
- Part of PostgreSQL's object access hook infrastructure for security and audit extensions
- The timing of this hook (after alteration but before command counter increment) allows extensions to see both old and new tuple versions
- Located in src/include/catalog/objectaccess.h:86-103