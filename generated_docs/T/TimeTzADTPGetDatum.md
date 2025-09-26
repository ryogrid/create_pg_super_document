# TimeTzADTPGetDatum

## Location
src/include/utils/date.h: 84 - 88

## Overview
TimeTzADTPGetDatum is a static inline function that converts a TimeTzADT pointer to a PostgreSQL Datum representation, enabling time with time zone values to be passed through the PostgreSQL function call interface.

## Definition


## Detailed Description
This function serves as a type conversion utility in PostgreSQL's internal data representation system. It takes a pointer to a TimeTzADT structure (time with time zone) and converts it to a Datum, which is PostgreSQL's universal data type used for passing values between functions in the SQL engine. The function is implemented as a simple wrapper around PointerGetDatum, casting the TimeTzADT pointer to a generic Datum type.

The function is part of PostgreSQL's date/time handling infrastructure and follows the standard pattern for converting complex data types to Datum representation. This conversion is essential for the PostgreSQL function call mechanism, which uses Datum as the standard parameter and return type for all SQL-callable functions.

## Parameters / Member Variables
- : A const pointer to a TimeTzADT structure representing a time value with time zone information

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (from src/include/postgres.h)
  - TimeTzADT (struct type defined in src/include/utils/date.h)

- Called from (representative examples):
  - PG_RETURN_TIMETZADT_P (macro in src/include/utils/date.h)
  - ExecEvalSQLValueFunction (src/backend/executor/execExprInterp.c:2657)
  - parse_datetime (src/backend/utils/adt/formatting.c:4588)
  - executeDateTimeMethod (src/backend/utils/adt/jsonpath_exec.c:2627)

## Notes and Other Information
- This is a static inline function defined in src/include/utils/date.h:84-88
- The function performs no validation or error checking - it assumes the input pointer is valid
- TimeTzADT contains two fields: a TimeADT (time portion) and an int32 (timezone offset in seconds)
- The function is commonly used through the PG_RETURN_TIMETZADT_P macro for returning time with timezone values from PostgreSQL functions
- As an inline function, it generates efficient code with minimal overhead for the type conversion