# attnumTypeId

## Location
src/backend/parser/parse_relation.c: 3555 - 3574

## Overview
This function retrieves the data type OID for a given attribute number within an opened relation.

## Definition


## Detailed Description
The  function returns the type OID (Object Identifier) of an attribute specified by its attribute number. For system attributes (attid <= 0), it uses  to get the system attribute's type information. For regular user attributes (attid > 0), it accesses the relation's tuple descriptor to retrieve the  field. The function performs bounds checking and will throw an ERROR if an invalid attribute number is provided. This is essential for type checking and type resolution during query planning and execution. Like other relation-specific functions, it should only be used on relations that are already opened with .

## Parameters / Member Variables
- : The opened relation to query
- : The attribute number whose type is requested (can be positive for user attributes or negative for system attributes)

## Dependencies
- Functions called/Symbols referenced:
  - SystemAttributeDefinition
  - TupleDescAttr
  - elog (for error handling)
- Called from (representative examples):
  - transformFkeyGetPrimaryKey
  - transformAssignedExpr
  - RIAttType

## Notes and Other Information
- Returns an Oid representing the PostgreSQL type system identifier
- Handles both system attributes (negative attid) and user attributes (positive attid)
- Throws ERROR for invalid attribute numbers beyond the relation's attribute count
- Uses 1-based indexing for user attributes (subtracts 1 when accessing rd_att array)
- Should only be used with already opened relations - use cache version get_atttype() for non-opened relations
- Essential for type compatibility checking in query processing