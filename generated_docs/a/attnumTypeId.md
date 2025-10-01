# attnumTypeId

## Location
[src/backend/parser/parse_relation.c:3555-3574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3555-L3574)

## Overview
This function retrieves the data type OID for a given attribute number within an opened relation.

## Definition

```c
Oid
attnumTypeId(Relation rd, int attid)
```
## Detailed Description
The  function returns the type OID (Object Identifier) of an attribute specified by its attribute number. For system attributes (attid <= 0), it uses  to get the system attribute's type information. For regular user attributes (attid > 0), it accesses the relation's tuple descriptor to retrieve the  field. The function performs bounds checking and will throw an ERROR if an invalid attribute number is provided. This is essential for type checking and type resolution during query planning and execution. Like other relation-specific functions, it should only be used on relations that are already opened with .

## Parameters / Member Variables
- : The opened relation to query
- : The attribute number whose type is requested (can be positive for user attributes or negative for system attributes)

## Dependencies
- Functions called/Symbols referenced:
  - [SystemAttributeDefinition](../S/SystemAttributeDefinition.md)
  - TupleDescAttr
  - elog (for error handling)
- Called from (representative examples):
  - [transformFkeyGetPrimaryKey](../t/transformFkeyGetPrimaryKey.md)
  - [transformAssignedExpr](../t/transformAssignedExpr.md)
  - RIAttType

## Notes and Other Information
- Returns an Oid representing the PostgreSQL type system identifier
- Handles both system attributes (negative attid) and user attributes (positive attid)
- Throws ERROR for invalid attribute numbers beyond the relation's attribute count
- Uses 1-based indexing for user attributes (subtracts 1 when accessing rd_att array)
- Should only be used with already opened relations - use cache version get_atttype() for non-opened relations
- Essential for type compatibility checking in query processing

## Simplified Source

```c
Oid
attnumTypeId(Relation rd, int attid)
{
    // Handle system attributes (negative attribute numbers)
    if (attid <= 0) {
        const FormData_pg_attribute *sysatt;
        sysatt = SystemAttributeDefinition(attid);
        return sysatt->atttypid;
    }

    // Validate user attribute number
    if (attid > rd->rd_att->natts)
        elog(ERROR, "invalid attribute number %d", attid);

    // Return type OID for user attribute (convert to 0-based index)
    return TupleDescAttr(rd->rd_att, attid - 1)->atttypid;
}
```