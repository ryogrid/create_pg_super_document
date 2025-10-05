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
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md): Resolves username to role OID
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

## Simplified Source

```c
Datum
has_sequence_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name     username = PG_GETARG_NAME(0);
    Oid      sequenceoid = PG_GETARG_OID(1);
    text    *priv_type_text = PG_GETARG_TEXT_PP(2);
    bool     is_missing = false;

    // Convert username to role OID
    Oid roleid = get_role_oid_or_public(NameStr(*username));

    // Parse privilege string to internal mode
    AclMode mode = convert_sequence_priv_string(priv_type_text);

    // Check if sequence exists and is actually a sequence
    char relkind = get_rel_relkind(sequenceoid);
    if (relkind == '\0')
        PG_RETURN_NULL();  // Object doesn't exist
    else if (relkind != RELKIND_SEQUENCE)
        ereport(ERROR, "not a sequence");

    // Check privilege and handle missing objects
    AclResult aclresult = pg_class_aclcheck_ext(sequenceoid, roleid, mode, &is_missing);

    if (is_missing)
        PG_RETURN_NULL();

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```