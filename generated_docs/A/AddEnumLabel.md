# AddEnumLabel

## Location
[src/backend/catalog/pg_enum.c:292-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L292-L606)

## Overview
Adds a new label to an existing enum type, with support for positioning the label before/after existing values and handling concurrent modifications through sophisticated OID allocation and transaction tracking.

## Definition

```c
void
AddEnumLabel(Oid enumTypeOid,
			 const char *newVal,
			 const char *neighbor,
			 bool newValIsAfter,
			 bool skipIfExists)
```
## Detailed Description
AddEnumLabel implements the core functionality for ALTER TYPE ADD VALUE operations in PostgreSQL. This complex function handles adding new enum values to existing enum types with several sophisticated features:

1. **Concurrent Access Control**: Uses ExclusiveLock on the enum type to prevent concurrent modifications while allowing read access by other backends.

2. **Positioning Logic**: Supports adding the new value at the end (default) or before/after a specified existing value using enumsortorder calculations.

3. **OID Allocation Strategy**: Implements intelligent OID allocation that prefers even-numbered OIDs for performance (enabling direct OID comparison), but falls back to odd OIDs when necessary to maintain correct sort order.

4. **Precision Handling**: Uses volatile float4 variables and renumbering logic to handle floating-point precision issues when inserting values between existing ones.

5. **Transaction Tracking**: Maintains uncommitted_enum_values hash table to track values added in the current transaction, supporting proper enum constraint enforcement.

6. **Binary Upgrade Support**: Special handling for pg_dump binary upgrade scenarios with predetermined OIDs.

## Parameters / Member Variables
- `enumTypeOid`: The OID of the enum type to add the value to
- `*newVal`: The string value of the new enum label to add
- `*neighbor`: Optional existing enum label to position relative to (NULL for end placement)
- `newValIsAfter`: When neighbor is specified, whether to place the new value after (true) or before (false) the neighbor
- `skipIfExists`: If true, skip with NOTICE rather than ERROR when label already exists
## Dependencies
- Functions called/Symbols referenced:
  - [LockDatabaseObject](../L/LockDatabaseObject.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - SearchSysCacheList1
  - qsort (with sort_order_cmp)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [RenumberEnumType](../R/RenumberEnumType.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [EnumTypeUncommitted](../E/EnumTypeUncommitted.md)
  - [init_uncommitted_enum_values](../i/init_uncommitted_enum_values.md)
  - [hash_search](../h/hash_search.md)
- Called from:
  - [AlterEnum](AlterEnum.md) (src/backend/commands/typecmds.c:1299)

## Notes and Other Information
- Uses enumsortorder values to maintain logical ordering separate from OID ordering
- The "restart" logic handles cases where float4 precision issues require renumbering all existing values
- Even/odd OID allocation strategy is a performance optimization for enum comparison operations
- Binary upgrade mode restricts BEFORE/AFTER positioning to maintain OID consistency
- Uncommitted enum value tracking is skipped for enum types created in the same transaction (optimization)
- The function validates label length against NAMEDATALEN before processing
- Extensive error handling for duplicate labels, invalid neighbors, and binary upgrade constraints

## Simplified Source

```c
void
AddEnumLabel(Oid enumTypeOid, const char *newVal, const char *neighbor,
             bool newValIsAfter, bool skipIfExists)
{
    Relation pg_enum;
    HeapTuple enum_tup;
    float4 newelemorder;
    HeapTuple *existing;
    CatCList *list;
    int nelems, i;
    Oid newOid;

    // Validate label length
    if (strlen(newVal) > (NAMEDATALEN - 1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
                        errmsg("invalid enum label \"%s\"", newVal)));

    // Lock enum type for exclusive access
    LockDatabaseObject(TypeRelationId, enumTypeOid, 0, ExclusiveLock);

    // Check if label already exists
    enum_tup = SearchSysCache2(ENUMTYPOIDNAME, ObjectIdGetDatum(enumTypeOid),
                               CStringGetDatum(newVal));
    if (HeapTupleIsValid(enum_tup))
    {
        ReleaseSysCache(enum_tup);
        if (skipIfExists)
        {
            ereport(NOTICE, (errmsg("enum label \"%s\" already exists, skipping", newVal)));
            return;
        }
        else
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("enum label \"%s\" already exists", newVal)));
    }

    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

restart:
    // Get existing enum members and sort by enumsortorder
    list = SearchSysCacheList1(ENUMTYPOIDNAME, ObjectIdGetDatum(enumTypeOid));
    nelems = list->n_members;
    existing = (HeapTuple *) palloc(nelems * sizeof(HeapTuple));
    for (i = 0; i < nelems; i++)
        existing[i] = &(list->members[i]->tuple);
    qsort(existing, nelems, sizeof(HeapTuple), sort_order_cmp);

    // Calculate sort order for new element
    if (neighbor == NULL)
    {
        // Add at end
        if (nelems > 0)
        {
            Form_pg_enum en = (Form_pg_enum) GETSTRUCT(existing[nelems - 1]);
            newelemorder = en->enumsortorder + 1;
        }
        else
            newelemorder = 1;
    }
    else
    {
        // Find neighbor and calculate position
        int nbr_index = -1;
        for (i = 0; i < nelems; i++)
        {
            Form_pg_enum en = (Form_pg_enum) GETSTRUCT(existing[i]);
            if (strcmp(NameStr(en->enumlabel), neighbor) == 0)
            {
                nbr_index = i;
                break;
            }
        }
        if (nbr_index < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("\"%s\" is not an existing enum label", neighbor)));

        // Calculate midpoint or edge position
        Form_pg_enum nbr_en = (Form_pg_enum) GETSTRUCT(existing[nbr_index]);
        int other_nbr_index = newValIsAfter ? nbr_index + 1 : nbr_index - 1;

        if (other_nbr_index < 0)
            newelemorder = nbr_en->enumsortorder - 1;
        else if (other_nbr_index >= nelems)
            newelemorder = nbr_en->enumsortorder + 1;
        else
        {
            Form_pg_enum other_nbr_en = (Form_pg_enum) GETSTRUCT(existing[other_nbr_index]);
            volatile float4 midpoint = (nbr_en->enumsortorder + other_nbr_en->enumsortorder) / 2;

            // Check for float precision issues
            if (midpoint == nbr_en->enumsortorder || midpoint == other_nbr_en->enumsortorder)
            {
                RenumberEnumType(pg_enum, existing, nelems);
                pfree(existing);
                ReleaseCatCacheList(list);
                goto restart;  // Try again with renumbered values
            }
            newelemorder = midpoint;
        }
    }

    // Allocate new OID with even preference for performance
    newOid = GetNewOidWithIndex(pg_enum, EnumOidIndexId, Anum_pg_enum_oid);

    // Create and insert new enum tuple
    Datum values[Natts_pg_enum];
    bool nulls[Natts_pg_enum];
    NameData enumlabel;

    memset(nulls, false, sizeof(nulls));
    values[Anum_pg_enum_oid - 1] = ObjectIdGetDatum(newOid);
    values[Anum_pg_enum_enumtypid - 1] = ObjectIdGetDatum(enumTypeOid);
    values[Anum_pg_enum_enumsortorder - 1] = Float4GetDatum(newelemorder);
    namestrcpy(&enumlabel, newVal);
    values[Anum_pg_enum_enumlabel - 1] = NameGetDatum(&enumlabel);

    enum_tup = heap_form_tuple(RelationGetDescr(pg_enum), values, nulls);
    CatalogTupleInsert(pg_enum, enum_tup);
    heap_freetuple(enum_tup);

    // Clean up
    pfree(existing);
    ReleaseCatCacheList(list);
    table_close(pg_enum, RowExclusiveLock);

    // Track uncommitted enum values for constraint enforcement
    if (uncommitted_enum_values == NULL)
        init_uncommitted_enum_values();
    hash_search(uncommitted_enum_values, &newOid, HASH_ENTER, NULL);
}
```