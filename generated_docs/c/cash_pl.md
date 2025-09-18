# cash_pl

## Location
src/backend/utils/adt/cash.c: 688 - 700

## Overview
The cash_pl function implements addition for PostgreSQL's cash (money) data type, safely adding two cash values together with overflow detection.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that implements the '+' operator for the cash data type. It extracts two cash values from the function arguments and delegates to the internal cash_pl_cash function for the actual arithmetic operation. The function follows PostgreSQL's standard function calling convention and includes overflow detection to prevent arithmetic errors that could result in incorrect monetary calculations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing two cash values to add
  - First argument (index 0): First cash value (addend)
  - Second argument (index 1): Second cash value (addend)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Macro to extract cash values from function arguments
  - [cash_pl_cash](cash_pl_cash.md): Internal static function that performs the actual addition with overflow checking
  - PG_RETURN_CASH: Macro to return cash result as Datum
- Called from (representative examples):
  - No direct references found (likely called through operator system)

## Notes and Other Information
- This function is part of PostgreSQL's cash data type implementation
- The cash data type is internally represented as a 64-bit integer
- Uses pg_add_s64_overflow for safe integer addition with overflow detection
- Will raise an ERROR with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE if overflow occurs
- This function is typically invoked through PostgreSQL's operator system rather than direct function calls
- Critical for financial calculations where accuracy and overflow protection are essential