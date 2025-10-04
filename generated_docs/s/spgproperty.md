# spgproperty

## Location
[src/backend/access/spgist/spgutils.c:1290-1360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L1290-L1360)

## Overview
Check boolean properties of SP-GiST indexes, specifically handling the AMPROP_DISTANCE_ORDERABLE property which is not supported by the core property code.

## Definition

```c
bool
spgproperty(Oid index_oid, int attno,
			IndexAMProperty prop, const char *propname,
			bool *res, bool *isnull)
```
## Detailed Description
The  function is a SP-GiST access method callback that determines boolean properties of indexes. This function is required for SP-GiST because the core PostgreSQL property code doesn't support the AMPROP_DISTANCE_ORDERABLE property, which is needed for distance-ordered scans.

The function specifically checks whether an SP-GiST index column supports distance-orderable operations by examining if there is a distance operator in the column's operator class with the default types. It performs this check by:

1. Retrieving the operator class for the specified column
2. Looking up the operator family and input data type
3. Searching for ordering operators (AMOP_ORDER) in the operator family
4. Verifying that the operator family can sort the return type of the operator

Currently, SP-GiST distance-ordered scans require a distance operator in the opclass with default types, so the presence of such an operator indicates support for distance-orderable operations.

## Parameters / Member Variables
- `index_oid`: The OID of the index being queried
- `attno`: The attribute number (column number) within the index (must be > 0)
- `prop`: The index property being queried (only AMPROP_DISTANCE_ORDERABLE is supported)
- `*propname`: The name of the property (for debugging/error purposes)
- `*res`: Output parameter - set to true if the property is supported, false otherwise
- `*isnull`: Output parameter - set to true if the property value is null/unknown
## Dependencies
- Functions called/Symbols referenced:
  - [get_index_column_opclass](../g/get_index_column_opclass.md)
  - [get_opclass_opfamily_and_input_type](../g/get_opclass_opfamily_and_input_type.md)
  - SearchSysCacheList1
  - [opfamily_can_sort_type](../o/opfamily_can_sort_type.md)
  - [get_op_rettype](../g/get_op_rettype.md)
  - ReleaseSysCacheList
  - AMPROP_DISTANCE_ORDERABLE
  - AMOP_ORDER
  - IndexAMProperty
  - [CatCList](../C/CatCList.md)
  - Form_pg_amop
- Called from (representative examples):
  - [spghandler](spghandler.md) (SP-GiST access method handler)

## Notes and Other Information
- Only handles column-level inquiries (attno > 0); returns false for index-level queries (attno == 0)
- Currently only supports the AMPROP_DISTANCE_ORDERABLE property; returns false for all other properties
- The function assumes that if a distance operator exists in the opclass, there's a valid reason for it and distance-ordered scans are supported
- Returns true with *isnull = true if the opclass or operator family information cannot be retrieved
- This is part of the SP-GiST (Space-Partitioned Generalized Search Tree) access method implementation
- The function is essential for enabling distance-ordered queries like KNN (k-nearest neighbor) searches in SP-GiST indexes

## Simplified Source

```c
bool
spgproperty(Oid index_oid, int attno,
            IndexAMProperty prop, const char *propname,
            bool *res, bool *isnull)
{
    Oid opclass, opfamily, opcintype;
    CatCList *catlist;
    int i;

    // Only handle column-level inquiries
    if (attno == 0)
        return false;

    // Only handle distance-orderable property
    switch (prop)
    {
        case AMPROP_DISTANCE_ORDERABLE:
            break;
        default:
            return false;
    }

    // Get column's operator class
    opclass = get_index_column_opclass(index_oid, attno);
    if (!OidIsValid(opclass))
    {
        *isnull = true;
        return true;
    }

    // Get operator family and input type
    if (!get_opclass_opfamily_and_input_type(opclass, &opfamily, &opcintype))
    {
        *isnull = true;
        return true;
    }

    // Search for distance operators in the operator family
    catlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamily));
    *res = false;

    for (i = 0; i < catlist->n_members; i++)
    {
        HeapTuple amoptup = &catlist->members[i]->tuple;
        Form_pg_amop amopform = (Form_pg_amop) GETSTRUCT(amoptup);

        // Check for ordering operator with compatible types
        if (amopform->amoppurpose == AMOP_ORDER &&
            (amopform->amoplefttype == opcintype ||
             amopform->amoprighttype == opcintype) &&
            opfamily_can_sort_type(amopform->amopsortfamily,
                                   get_op_rettype(amopform->amopopr)))
        {
            *res = true;
            break;
        }
    }

    ReleaseSysCacheList(catlist);
    *isnull = false;
    return true;
}
```