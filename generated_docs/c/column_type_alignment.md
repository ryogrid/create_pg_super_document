# column_type_alignment

## Location
src/fe_utils/print.c: 3614 - 3640

## Overview
The column_type_alignment function determines the appropriate text alignment (left or right) for table columns based on PostgreSQL data type OIDs, ensuring numeric types are right-aligned for better readability.

## Definition


## Detailed Description
This function implements a simple but important formatting rule for tabular data display: numeric data types should be right-aligned while all other data types should be left-aligned. The function takes a PostgreSQL data type OID (Object Identifier) and returns a character indicating the alignment preference ('r' for right, 'l' for left).

The function uses a switch statement to categorize data types, explicitly listing all numeric types that should be right-aligned including integers of various sizes, floating-point numbers, the NUMERIC type for arbitrary precision, object identifiers, transaction IDs, and the MONEY type. All other data types default to left alignment, which is appropriate for text, dates, booleans, and other non-numeric types.

## Parameters / Member Variables
- : PostgreSQL data type OID (Object Identifier) representing the column's data type

## Dependencies
- Functions called/Symbols referenced:
  - PostgreSQL type OID constants:
    - INT2OID (smallint/int2)
    - INT4OID (integer/int4)  
    - INT8OID (bigint/int8)
    - FLOAT4OID (real/float4)
    - FLOAT8OID (double precision/float8)
    - NUMERICOID (numeric/decimal)
    - OIDOID (oid type)
    - XIDOID (transaction id)
    - XID8OID (8-byte transaction id)
    - CIDOID (command id)
    - MONEYOID (money type)
- Called from (representative examples):
  - [printQuery](../p/printQuery.md) (when setting up column headers)
  - [printCrosstab](../p/printCrosstab.md) (for crosstab view column alignment)

## Notes and Other Information
- The function follows the common convention that numeric data should be right-aligned for easier visual comparison and reading of values
- The MONEY type is treated as numeric since it represents currency values that benefit from right alignment
- Transaction IDs and command IDs are considered numeric for alignment purposes as they are sequential identifiers
- The default case handles all non-numeric types including text, varchar, char, date, time, timestamp, boolean, bytea, arrays, and user-defined types
- This alignment information is typically used by table formatting functions to properly pad and position column content
- The simple character return value ('r' or 'l') makes it easy to integrate with existing table formatting systems