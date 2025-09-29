CheckAttributeType

## Overview
CheckAttributeType verifies that a proposed attribute data type is valid for use in PostgreSQL tables, performing recursive validation for complex types and enforcing restrictions on pseudo-types.

## Definition
void CheckAttributeType(const char *attname, Oid atttypid, Oid attcollation, List *containing_rowtypes, int flags)

## Detailed Description
CheckAttributeType is a comprehensive type validation function that ensures data types are suitable for table columns. It performs different validation strategies based on the type category: for pseudo-types, it enforces restrictions while allowing certain exceptions based on flags; for domain types, it recursively validates the base type; for composite types, it validates all component attributes while preventing infinite recursion; for range types, it validates the subtype; and for array types, it validates the element type.

The function includes sophisticated recursive containment detection to prevent self-referential composite types that would create infinite recursion. It also validates that collatable types have proper collation information. The validation is designed to catch type definition errors early in the relation creation process.

## Parameters / Member Variables
- attname: The name of the attribute being validated (used in error messages)
- atttypid: The OID of the data type to validate
- attcollation: The collation OID for the attribute (required for collatable types)
- containing_rowtypes: List of rowtype OIDs to detect recursive containment (pass NIL for new rowtypes)
- flags: Bitmask controlling validation behavior (CHKATYPE_ANYARRAY, CHKATYPE_ANYRECORD, CHKATYPE_IS_PARTKEY)

## Dependencies
- Functions called/Symbols referenced:
  - [get_typtype](../g/get_typtype.md) (determines type category)
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow in recursion)
  - [getBaseType](../g/getBaseType.md) (gets base type for domains)
  - [get_typ_typrelid](../g/get_typ_typrelid.md) (gets relation OID for composite types)
  - [relation_open](../r/relation_open.md), relation_close (access composite type definitions)
  - [get_range_subtype](../g/get_range_subtype.md), get_range_collation (range type introspection)
  - [get_element_type](../g/get_element_type.md) (array element type introspection)
  - [type_is_collatable](../t/type_is_collatable.md) (determines if type requires collation)
  - [list_member_oid](../l/list_member_oid.md), lappend_oid, list_delete_last (list manipulation for recursion tracking)
- Called from (representative examples):
  - [CheckAttributeNamesTypes](CheckAttributeNamesTypes.md) (in src/backend/catalog/heap.c:512)
  - [CheckAttributeType](CheckAttributeType.md) (recursive calls at multiple lines)
  - [ConstructTupleDescriptor](ConstructTupleDescriptor.md) (in src/backend/catalog/index.c:410)
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (in src/backend/commands/tablecmds.c:7177)

## Notes and Other Information
- Recursively validates complex types including composites, domains, ranges, and arrays
- Prevents infinite recursion in composite types through containing_rowtypes tracking
- Allows certain pseudo-types (ANYARRAY, RECORD, RECORDARRAY) based on flags
- Provides different error message phrasing for partition key columns vs regular columns
- Enforces collation requirements for collatable types
- Stack depth checking prevents stack overflow during deep recursion
- Critical for maintaining type system integrity in PostgreSQL
- Located in src/backend/catalog/heap.c:549-681

## Simplified Source

```c
void
CheckAttributeType(const char *attname, Oid atttypid, Oid attcollation,
                   List *containing_rowtypes, int flags)
{
    char att_typtype = get_typtype(atttypid);
    Oid att_typelem;

    // Prevent infinite recursion
    check_stack_depth();

    if (att_typtype == TYPTYPE_PSEUDO)
    {
        // Disallow pseudo-types except for allowed ones based on flags
        if (!((atttypid == ANYARRAYOID && (flags & CHKATYPE_ANYARRAY)) ||
              (atttypid == RECORDOID && (flags & CHKATYPE_ANYRECORD)) ||
              (atttypid == RECORDARRAYOID && (flags & CHKATYPE_ANYRECORD))))
        {
            if (flags & CHKATYPE_IS_PARTKEY)
                ereport(ERROR, "Partition key column has pseudo-type");
            else
                ereport(ERROR, "Column has pseudo-type");
        }
    }
    else if (att_typtype == TYPTYPE_DOMAIN)
    {
        // Recurse to validate domain base type
        CheckAttributeType(attname, getBaseType(atttypid), attcollation,
                           containing_rowtypes, flags);
    }
    else if (att_typtype == TYPTYPE_COMPOSITE)
    {
        // Check for self-containment to prevent infinite recursion
        if (list_member_oid(containing_rowtypes, atttypid))
            ereport(ERROR, "Composite type cannot be made a member of itself");

        containing_rowtypes = lappend_oid(containing_rowtypes, atttypid);

        // Open composite type relation and validate all attributes
        Relation relation = relation_open(get_typ_typrelid(atttypid), AccessShareLock);
        TupleDesc tupdesc = RelationGetDescr(relation);

        for (int i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

            if (attr->attisdropped)
                continue;

            CheckAttributeType(NameStr(attr->attname),
                               attr->atttypid, attr->attcollation,
                               containing_rowtypes,
                               flags & ~CHKATYPE_IS_PARTKEY);
        }

        relation_close(relation, AccessShareLock);
        containing_rowtypes = list_delete_last(containing_rowtypes);
    }
    else if (att_typtype == TYPTYPE_RANGE)
    {
        // Recurse to validate range subtype
        CheckAttributeType(attname, get_range_subtype(atttypid),
                           get_range_collation(atttypid),
                           containing_rowtypes, flags);
    }
    else if (OidIsValid((att_typelem = get_element_type(atttypid))))
    {
        // Recurse to validate array element type
        CheckAttributeType(attname, att_typelem, attcollation,
                           containing_rowtypes, flags);
    }

    // Validate collation for collatable types
    if (!OidIsValid(attcollation) && type_is_collatable(atttypid))
    {
        if (flags & CHKATYPE_IS_PARTKEY)
            ereport(ERROR, "No collation derived for partition key column with collatable type");
        else
            ereport(ERROR, "No collation derived for column with collatable type");
    }
}
```