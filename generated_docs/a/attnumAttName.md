# attnumAttName

## Location
src/backend/parser/parse_relation.c: 3533 - 3554

## Overview
This function retrieves the attribute name for a given attribute number (attnum) within an opened relation.

## Definition


## Detailed Description
The  function performs the reverse lookup of  - it takes an attribute number and returns the corresponding attribute name. For system attributes (attid <= 0), it uses  to get the system attribute information. For regular user attributes (attid > 0), it accesses the relation's tuple descriptor to retrieve the attribute name. The function performs bounds checking and will throw an ERROR if an invalid attribute number is provided. Like other relation-specific functions in this module, it should only be used on relations that are already opened with .

## Parameters / Member Variables
- : The opened relation to search within
- : The attribute number to look up (can be positive for user attributes or negative for system attributes)

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