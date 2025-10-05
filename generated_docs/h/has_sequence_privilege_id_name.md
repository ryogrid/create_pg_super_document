# has_sequence_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:2240-2267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2240-L2267)

## Overview
Checks user privileges on a sequence given a role OID, sequence name as text, and text privilege name.

## Definition
```c
Datum has_sequence_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that verifies whether a specific user (identified by role OID) has the requested privileges on a sequence object identified by name. It takes three arguments: a role OID, a sequence name as text, and a privilege specification as text. The function converts the sequence name to its object identifier, validates that the target object is indeed a sequence, converts the privilege string to an internal privilege mode representation, and performs an access control check. Unlike the other variants, this function uses convert_table_name to resolve the sequence name to its OID and uses pg_class_aclcheck instead of the extended version. It returns a boolean result indicating whether the specified user has the requested privileges.

## Parameters / Member Variables
- `PG_GETARG_OID(0)`: Role OID of the user for which to check privileges
- `PG_GETARG_TEXT_PP(1)`: Text string containing the sequence name
- `PG_GETARG_TEXT_PP(2)`: Text string specifying the privilege type to check

## Dependencies
- Functions called/Symbols referenced:
  - [convert_sequence_priv_string](../c/convert_sequence_priv_string.md): Converts privilege text to AclMode bitmask
  - [convert_table_name](../c/convert_table_name.md): Converts sequence name text to OID
  - [get_rel_relkind](../g/get_rel_relkind.md): Gets relation kind to verify it's a sequence
  - [text_to_cstring](../t/text_to_cstring.md): Converts text to C string for error messages
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md): Performs the actual privilege check
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Throws an error if the specified name refers to a non-sequence object
- Uses convert_table_name for name resolution, which can handle schema-qualified names
- Uses pg_class_aclcheck instead of pg_class_aclcheck_ext (doesn't handle missing objects with NULL return)
- Part of PostgreSQL's privilege checking system for sequence objects
- Defined in src/backend/utils/adt/acl.c:2240-2267

## Simplified Source

```c
Datum
has_sequence_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid   roleid = PG_GETARG_OID(0);
    text *sequencename = PG_GETARG_TEXT_PP(1);
    text *priv_type_text = PG_GETARG_TEXT_PP(2);

    // Parse privilege string to internal mode
    AclMode mode = convert_sequence_priv_string(priv_type_text);

    // Resolve sequence name to OID
    Oid sequenceoid = convert_table_name(sequencename);

    // Validate target is actually a sequence
    if (get_rel_relkind(sequenceoid) != RELKIND_SEQUENCE)
        ereport(ERROR, "not a sequence");

    // Check privilege and return result
    AclResult aclresult = pg_class_aclcheck(sequenceoid, roleid, mode);
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```