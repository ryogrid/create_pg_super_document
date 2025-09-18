# expand_col_privileges

## Location
[src/backend/catalog/aclchk.c:1601-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1601-L1633)

## Overview
Expands specified privileges into per-column array entries for each specified attribute in a relation.

## Definition
```c
static void expand_col_privileges(List *colnames, Oid table_oid,
                                 AclMode this_privileges,
                                 AclMode *col_privileges,
                                 int num_col_privileges)
```

## Detailed Description
This static function processes a list of column names and applies specified privileges to those columns by OR-ing the privileges into a per-column privileges array. The function validates each column name exists in the relation and maps it to the appropriate array index. The per-column array is indexed starting at FirstLowInvalidHeapAttributeNumber offset up to the relation's last attribute.

The function performs column name validation and converts column names to attribute numbers, then applies the privileges by bitwise OR operation into the appropriate array position.

## Parameters
- `colnames`: List of column names (as string values) to which privileges should be applied
- `table_oid`: OID of the table/relation containing the columns
- `this_privileges`: AclMode bitmask representing the privileges to be applied to the specified columns
- `col_privileges`: Array to store per-column privileges, indexed by adjusted attribute number
- `num_col_privileges`: Size of the col_privileges array

## Dependencies
- Functions called/Symbols referenced:
  - [get_attnum](../g/get_attnum.md)
  - [get_rel_name](../g/get_rel_name.md)
  - strVal
  - lfirst
  - InvalidAttrNumber
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md)

## Notes and Other Information
- This is a static function only used within aclchk.c
- The function performs bounds checking to ensure column numbers are within the valid range
- Uses FirstLowInvalidHeapAttributeNumber as the base offset for array indexing to handle system columns
- Throws an error if a specified column name does not exist in the relation
- The privileges are applied using bitwise OR, allowing multiple privilege operations to accumulate