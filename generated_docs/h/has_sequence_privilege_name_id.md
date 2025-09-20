# has_sequence_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:2168-2204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2168-L2204)

## Overview
Checks user privileges on a sequence given a username, sequence OID, and text privilege name.

## Definition

```c
Datum
has_sequence_privilege_name_id(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL built-in function that verifies whether a specific user has the requested privileges on a sequence object. It takes three arguments: a username (Name type), a sequence object identifier (OID), and a privilege specification as text. The function validates that the target object is indeed a sequence, converts the privilege string to an internal privilege mode representation, and performs an access control check. It returns a boolean result indicating whether the user has the specified privileges, or NULL if the sequence doesn't exist.

## Parameters / Member Variables
- : Username for which to check privileges
- : Object identifier of the sequence to check
- : Text string specifying the privilege type to check

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Resolves username to role OID
  - [convert_sequence_priv_string](../c/convert_sequence_priv_string.md): Converts privilege text to AclMode bitmask
  - [get_rel_relkind](../g/get_rel_relkind.md): Gets relation kind to verify it's a sequence
  - [get_rel_name](../g/get_rel_name.md): Gets relation name for error messages
  - [pg_class_aclcheck_ext](../p/pg_class_aclcheck_ext.md): Performs the actual privilege check
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Returns NULL if the sequence object doesn't exist
- Throws an error if the specified OID refers to a non-sequence object
- Part of PostgreSQL's privilege checking system for sequence objects
- Defined in src/backend/utils/adt/acl.c:2168-2204