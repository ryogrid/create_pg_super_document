# attnumCollationId

## Location
src/backend/parser/parse_relation.c: 3575 - 3593

## Overview
This function retrieves the collation OID for a given attribute number within an opened relation.

## Definition


## Detailed Description
The  function returns the collation OID (Object Identifier) of an attribute specified by its attribute number. Collations determine the sorting and comparison rules for text data types. For system attributes (attid <= 0), the function returns InvalidOid since all system attributes are of non-collatable types. For regular user attributes (attid > 0), it accesses the relation's tuple descriptor to retrieve the  field. The function performs bounds checking and will throw an ERROR if an invalid attribute number is provided. This is crucial for proper text comparison and sorting operations in queries involving collatable data types. Like other relation-specific functions, it should only be used on relations that are already opened with .

## Parameters / Member Variables
- : The opened relation to query
- : The attribute number whose collation is requested (must be positive for user attributes)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - elog (for error handling)
  - InvalidOid (constant)
- Called from (representative examples):
  - RIAttCollation

## Notes and Other Information
- Returns an Oid representing the PostgreSQL collation identifier, or InvalidOid if not collatable
- System attributes always return InvalidOid as they are non-collatable
- Throws ERROR for invalid attribute numbers beyond the relation's attribute count
- Uses 1-based indexing for user attributes (subtracts 1 when accessing rd_att array)
- Should only be used with already opened relations
- Essential for proper text comparison operations and ORDER BY clauses involving text columns
- Only meaningful for collatable data types (text, varchar, char, etc.)