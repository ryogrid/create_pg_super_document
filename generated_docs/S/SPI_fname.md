# SPI_fname

## Location
src/backend/executor/spi.c: 1198 - 1219

## Overview
Returns the name of an attribute given its attribute number in a tuple descriptor, supporting both regular and system attributes.

## Definition


## Detailed Description
SPI_fname performs the reverse operation of SPI_fnumber: given an attribute number, it returns the corresponding attribute name. The function handles both regular user-defined attributes (positive numbers) and system attributes (negative numbers). For regular attributes, it uses 1-based indexing where 1 corresponds to the first attribute in the tuple descriptor. For system attributes, it uses the PostgreSQL system attribute numbering scheme.

The function validates the attribute number to ensure it's within valid bounds and returns a newly allocated copy of the attribute name string. The caller is responsible for freeing the returned string.

## Parameters / Member Variables
- : The TupleDesc structure containing attribute information for the tuple
- : The attribute number to look up (1-based for regular attributes, negative for system attributes)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro for accessing tuple descriptor attributes)
  - [SystemAttributeDefinition](SystemAttributeDefinition.md) (for looking up system attribute definitions)
  - [pstrdup](../p/pstrdup.md) (for duplicating the attribute name string)
  - NameStr (macro for extracting string from Name type)
- Called from (representative examples):
  - [SPI_sql_row_to_xmlelement](SPI_sql_row_to_xmlelement.md) (XML utility function)
  - Various stored procedures and functions that need attribute names dynamically

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Sets SPI_result to SPI_ERROR_NOATTRIBUTE if fnumber is invalid
- Accepts 1-based attribute numbers for regular attributes (1, 2, 3, ...)
- Accepts negative attribute numbers for system attributes (e.g., -1, -2, ...)
- Returns NULL if the attribute number is 0, greater than natts, or less than FirstLowInvalidHeapAttributeNumber
- System attributes include ctid, oid, xmin, xmax, cmin, cmax, tableoid
- The returned string is a copy, so modifications won't affect the original tuple descriptor
- Useful for dynamic schema inspection and error reporting in stored procedures