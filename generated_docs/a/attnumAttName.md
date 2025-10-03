# attnumAttName

## Location
[src/backend/parser/parse_relation.c:3533-3554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3533-L3554)

## Overview
This function retrieves the attribute name for a given attribute number (attnum) within an opened relation.

## Definition

```c
const NameData *
attnumAttName(Relation rd, int attid)
```
## Detailed Description
The  function performs the reverse lookup of  - it takes an attribute number and returns the corresponding attribute name. For system attributes (attid <= 0), it uses  to get the system attribute information. For regular user attributes (attid > 0), it accesses the relation's tuple descriptor to retrieve the attribute name. The function performs bounds checking and will throw an ERROR if an invalid attribute number is provided. Like other relation-specific functions in this module, it should only be used on relations that are already opened with .

## Parameters / Member Variables
- `rd`: The opened relation to search within
- `attid`: The attribute number to look up (can be positive for user attributes or negative for system attributes)
## Dependencies
- Functions called/Symbols referenced:
  - [SystemAttributeDefinition](../S/SystemAttributeDefinition.md)
  - TupleDescAttr
  - elog (for error handling)
- Called from (representative examples):
  - [transformFkeyGetPrimaryKey](../t/transformFkeyGetPrimaryKey.md)
  - RIAttName

## Notes and Other Information
- Returns a pointer to NameData structure containing the attribute name
- Handles both system attributes (negative attid) and user attributes (positive attid)
- Throws ERROR for invalid attribute numbers beyond the relation's attribute count
- Uses 1-based indexing for user attributes (subtracts 1 when accessing rd_att array)
- Should only be used with already opened relations - use cache version get_atttype() for non-opened relations

## Simplified Source

```c
const NameData *
attnumAttName(Relation rd, int attid)
{
    // Handle system attributes (negative attribute numbers)
    if (attid <= 0)
    {
        const FormData_pg_attribute *sysatt;
        sysatt = SystemAttributeDefinition(attid);
        return &sysatt->attname;
    }

    // Validate attribute number for user attributes
    if (attid > rd->rd_att->natts)
        elog(ERROR, "invalid attribute number %d", attid);

    // Return user attribute name (convert 1-based to 0-based indexing)
    return &TupleDescAttr(rd->rd_att, attid - 1)->attname;
}
```