# format_operator_extended

## Location
src/backend/utils/adt/regproc.c: 722 - 792

## Overview
Converts an operator OID to a textual representation in the format "opr_name(args)", providing extended formatting options and error handling capabilities.

## Definition


## Detailed Description
The  function is the core operator formatting function that converts an operator OID to its textual representation. It provides the underlying functionality used by other operator formatting functions and offers flexible control over the output format through flag parameters.

The function queries the pg_operator system catalog to retrieve operator information and constructs a string in the format "opr_name(lefttype,righttype)" or "opr_name(NONE,righttype)" for unary operators. It handles schema qualification based on visibility rules and flag settings, and can optionally return NULL for invalid operator OIDs.

## Parameters / Member Variables
- : The OID of the operator to format
- : Bit flags controlling the formatting behavior:
  - : Return NULL for invalid/unknown operator OIDs instead of numeric representation
  - : Always include schema qualification regardless of search_path visibility

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - IsBootstrapProcessingMode
  - initStringInfo
  - OperatorIsVisible
  - get_namespace_name
  - quote_identifier
  - appendStringInfo
  - appendStringInfoString
  - format_type_be_qualified
  - format_type_be
  - ReleaseSysCache
  - palloc
  - snprintf
- Called from (representative examples):
  - getObjectDescription (src/backend/catalog/objectaddress.c:3143)
  - getObjectIdentityParts (src/backend/catalog/objectaddress.c:5037)
  - format_operator (src/backend/utils/adt/regproc.c:795)
  - format_operator_qualified (src/backend/utils/adt/regproc.c:801)

## Notes and Other Information
- This is the primary implementation function for operator formatting in PostgreSQL
- Handles both binary and unary operators, representing missing operands as "NONE"
- Uses StringInfo buffer for efficient string construction
- Includes schema qualification logic based on operator visibility in the current search_path
- Bootstrap mode is not supported (assertion check included)
- Returns a palloc'd string that must be freed by the caller
- For invalid OIDs, either returns NULL or the numeric OID as a string depending on flags
- Located in src/backend/utils/adt/regproc.c:722-792