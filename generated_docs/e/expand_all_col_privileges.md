# expand_all_col_privileges

## Location
[src/backend/catalog/aclchk.c:1634-1679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1634-L1679)

## Overview
Expands specified privileges into per-column array entries for all valid attributes of a relation.

## Definition
```c
static void expand_all_col_privileges(Oid table_oid, Form_pg_class classForm,
                                     AclMode this_privileges,
                                     AclMode *col_privileges,
                                     int num_col_privileges)
```

## Detailed Description
This static function applies specified privileges to all valid columns of a relation by OR-ing the privileges into a per-column privileges array. Unlike expand_col_privileges which operates on specific named columns, this function processes all existing, non-dropped columns in the relation.

The function iterates through all possible attribute numbers from FirstLowInvalidHeapAttributeNumber+1 to the relation's maximum attribute number, checks each attribute's validity and dropped status, and applies privileges to valid columns. It handles special cases like views (which don't have system columns) and skips dropped columns.

## Parameters
- `table_oid`: OID of the table/relation whose columns will receive privileges
- `classForm`: Form_pg_class structure containing relation metadata including attribute count
- `this_privileges`: AclMode bitmask representing the privileges to be applied to all valid columns
- `col_privileges`: Array to store per-column privileges, indexed by adjusted attribute number
- `num_col_privileges`: Size of the col_privileges array for bounds checking

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - GETSTRUCT
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int16GetDatum](../I/Int16GetDatum.md)
  - HeapTupleIsValid
  - FirstLowInvalidHeapAttributeNumber
  - InvalidAttrNumber
  - RELKIND_VIEW
- Called from (representative examples):
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)

## Notes and Other Information
- This is a static function only used within aclchk.c
- Includes an assertion to verify the relation's attribute count fits within the provided array bounds
- Skips invalid attribute numbers and dropped columns to avoid applying privileges to non-existent attributes
- Special handling for views which do not have system columns (negative attribute numbers)
- Uses system cache lookups to verify attribute existence and check if attributes are dropped
- The privileges are applied using bitwise OR, allowing multiple privilege operations to accumulate
- Array indexing accounts for FirstLowInvalidHeapAttributeNumber offset to handle system columns properly

## Simplified Source

```c
static void
expand_all_col_privileges(Oid table_oid, Form_pg_class classForm,
                          AclMode this_privileges,
                          AclMode *col_privileges,
                          int num_col_privileges)
{
    AttrNumber curr_att;

    // Safety check: ensure relation fits in our array
    Assert(classForm->relnatts - FirstLowInvalidHeapAttributeNumber < num_col_privileges);

    // Iterate through all possible attribute numbers
    for (curr_att = FirstLowInvalidHeapAttributeNumber + 1;
         curr_att <= classForm->relnatts;
         curr_att++) {
        HeapTuple attTuple;
        bool isdropped;

        // Skip invalid attribute numbers
        if (curr_att == InvalidAttrNumber)
            continue;

        // Views don't have system columns (negative attribute numbers)
        if (classForm->relkind == RELKIND_VIEW && curr_att < 0)
            continue;

        // Look up attribute information in system catalog
        attTuple = SearchSysCache2(ATTNUM,
                                  ObjectIdGetDatum(table_oid),
                                  Int16GetDatum(curr_att));
        if (!HeapTupleIsValid(attTuple))
            elog(ERROR, "cache lookup failed for attribute %d of relation %u",
                 curr_att, table_oid);

        // Check if column is dropped
        isdropped = ((Form_pg_attribute) GETSTRUCT(attTuple))->attisdropped;
        ReleaseSysCache(attTuple);

        // Skip dropped columns
        if (isdropped)
            continue;

        // Apply privileges to this valid column
        // Array is offset by FirstLowInvalidHeapAttributeNumber
        col_privileges[curr_att - FirstLowInvalidHeapAttributeNumber] |= this_privileges;
    }
}
```