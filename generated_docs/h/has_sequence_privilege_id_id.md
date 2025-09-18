# has_sequence_privilege_id_id

## Location
src/backend/utils/adt/acl.c: 2268 - 2300

## Overview
Checks user privileges on a sequence given a role OID, sequence OID, and text privilege name.

## Definition
```c
Datum has_sequence_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that verifies whether a specific user (identified by role OID) has the requested privileges on a sequence object (identified by sequence OID). It takes three arguments: a role OID, a sequence object identifier, and a privilege specification as text. This is the most direct variant of the sequence privilege checking functions, as both the user and sequence are identified by their OIDs without requiring name resolution. The function validates that the target object is indeed a sequence, converts the privilege string to an internal privilege mode representation, and performs an access control check. It returns a boolean result indicating whether the specified user has the requested privileges, or NULL if the sequence doesn't exist.

## Parameters / Member Variables
- `PG_GETARG_OID(0)`: Role OID of the user for which to check privileges
- `PG_GETARG_OID(1)`: Object identifier of the sequence to check
- `PG_GETARG_TEXT_PP(2)`: Text string specifying the privilege type to check

## Dependencies
- Functions called/Symbols referenced:
  - convert_sequence_priv_string: Converts privilege text to AclMode bitmask
  - get_rel_relkind: Gets relation kind to verify it's a sequence
  - get_rel_name: Gets relation name for error messages
  - pg_class_aclcheck_ext: Performs the actual privilege check
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL if the sequence object doesn't exist
- Throws an error if the specified OID refers to a non-sequence object
- Most efficient variant as it avoids name resolution for both user and sequence
- Uses pg_class_aclcheck_ext which properly handles missing objects
- Part of PostgreSQL's privilege checking system for sequence objects
- Defined in src/backend/utils/adt/acl.c:2268-2300