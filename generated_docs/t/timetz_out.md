# timetz_out

## Location
[src/backend/utils/adt/date.c:2314-2334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2314-L2334)

## Overview
The timetz_out function is PostgreSQL's output function for the TIME WITH TIME ZONE data type, responsible for converting internal TimeTzADT values into their human-readable string representations.

## Definition
```c
Datum timetz_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the primary output formatter for TIME WITH TIME ZONE values in PostgreSQL. It takes an internal TimeTzADT structure and converts it to a string representation suitable for display to users or for external data exchange.

The conversion process follows these steps:
1. Extract the TimeTzADT value from the function arguments
2. Convert the internal representation to broken-down time components using timetz2tm
3. Format the time components into a string using EncodeTimeOnly, which respects the current DateStyle setting
4. Create a duplicate string using pstrdup for proper memory management
5. Return the formatted string as a C string

The output format depends on PostgreSQL's DateStyle setting and includes both the time portion and the timezone offset in an appropriate format.

## Parameters / Member Variables
- `time`: The input TimeTzADT value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P
  - [timetz2tm](timetz2tm.md)
  - [EncodeTimeOnly](../E/EncodeTimeOnly.md)
  - [pstrdup](../p/pstrdup.md)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md)
  - No other direct callers found (typically invoked through PostgreSQL's type system during result formatting)

## Notes and Other Information
- This is the primary output function for the TIME WITH TIME ZONE data type in PostgreSQL
- Located in src/backend/utils/adt/date.c as part of the date/time ADT implementations
- The output format is controlled by the DateStyle global variable, allowing different display styles
- Uses pstrdup to ensure proper memory management of the result string within PostgreSQL's memory context system
- The function handles timezone formatting automatically based on the stored timezone offset
- Typically invoked automatically by PostgreSQL when TIME WITH TIME ZONE values need to be displayed or converted to text
- The resulting string format is suitable for both human reading and for input back into timetz_in

## Simplified Source

```c
Datum timetz_out(PG_FUNCTION_ARGS) {
    TimeTzADT *time = PG_GETARG_TIMETZADT_P(0);
    char *result;
    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;
    int tz;
    char buf[MAXDATELEN + 1];

    // Convert internal time structure to broken-down time
    timetz2tm(time, tm, &fsec, &tz);

    // Format time with timezone to string buffer
    EncodeTimeOnly(tm, fsec, true, tz, DateStyle, buf);

    // Return palloc'd copy of formatted string
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}
```