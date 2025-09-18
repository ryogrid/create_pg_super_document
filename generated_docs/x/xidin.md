# xidin

## Location
src/backend/utils/adt/xid.c: 32 - 41

## Overview
The xidin function is an input conversion function that parses a string representation of a transaction ID (XID) and converts it into PostgreSQL's internal TransactionId type.

## Definition


## Detailed Description
xidin serves as the input conversion function for the xid data type in PostgreSQL's type system. It takes a C-string containing a numeric representation of a transaction ID and converts it to PostgreSQL's internal TransactionId format. The function uses the generic uint32in_subr utility function to perform the actual string-to-integer conversion, which handles error checking and validation of the input string format.

## Parameters / Member Variables
- : C-string containing the textual representation of the transaction ID to be converted
- : Internal variable storing the converted TransactionId value

## Dependencies
- Functions called/Symbols referenced:
  - uint32in_subr
  - PG_RETURN_TRANSACTIONID
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's type input/output system for the xid data type
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Relies on uint32in_subr for robust string parsing and error handling
- The function is registered in the system catalogs as the input function for the xid type