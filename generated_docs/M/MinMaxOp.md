# MinMaxOp

## Location
src/include/nodes/primnodes.h: 1504 - 1505

## Overview
MinMaxOp is an enumeration that specifies the operation type for MinMaxExpr nodes, distinguishing between GREATEST and LEAST functions in PostgreSQL.

## Definition


## Detailed Description
MinMaxOp defines the two types of min/max operations supported by PostgreSQL's MinMaxExpr expression nodes. This enumeration is used to specify whether a MinMaxExpr should compute the greatest (maximum) or least (minimum) value among its arguments.

The enumeration supports:
- **IS_GREATEST**: Represents the GREATEST function which returns the largest value from a set of arguments
- **IS_LEAST**: Represents the LEAST function which returns the smallest value from a set of arguments

These operations are commonly used in SQL for finding the maximum or minimum value across multiple columns or expressions in a single row, which is different from aggregate functions like MAX() and MIN() that operate across multiple rows.

## Parameters / Member Variables
- : Specifies GREATEST operation (returns maximum value among arguments)
- : Specifies LEAST operation (returns minimum value among arguments)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this enum)
- Called from (representative examples):
  - MinMaxExpr struct (uses MinMaxOp as op field)
  - [ExecEvalMinMax](../E/ExecEvalMinMax.md) function
  - ExprEvalStep struct

## Notes and Other Information
- Used as the op field in MinMaxExpr structures to specify the min/max operation type
- MinMaxExpr handles type coercion and collation for comparing arguments of potentially different but compatible types
- The corresponding MinMaxExpr structure includes fields for result type (minmaxtype), collation information (minmaxcollid, inputcollid), and the argument list
- Essential for implementing SQL GREATEST and LEAST functions which find extreme values within a single row
- Different from aggregate MIN/MAX functions which operate across multiple rows
- Arguments are evaluated and compared using the appropriate comparison operators for their data types
- Supports any number of arguments through the List structure in MinMaxExpr