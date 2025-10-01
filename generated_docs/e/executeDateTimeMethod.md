# executeDateTimeMethod

## Location
[src/backend/utils/adt/jsonpath_exec.c:2339-2819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2339-L2819)

## Overview
Implements JSON path datetime methods (.datetime(), .date(), .time(), .time_tz(), .timestamp(), .timestamp_tz()) that convert string values to PostgreSQL datetime types.

## Definition
```c
static JsonPathExecResult executeDateTimeMethod(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
```

## Detailed Description
The `executeDateTimeMethod` function provides comprehensive datetime conversion functionality for JSON path expressions in PostgreSQL. It converts string representations of dates and times into appropriate PostgreSQL datetime types based on the specific method called. The function supports both template-based parsing (for .datetime()) and ISO format parsing (for other methods). It handles type conversions between different datetime types, validates input formats, applies optional time precision parameters, and manages timezone information appropriately.

## Parameters / Member Variables  
- `cxt`: JsonPathExecContext pointer providing execution context and timezone usage settings
- `jsp`: JsonPathItem pointer representing the datetime method being executed
- `jb`: JsonbValue pointer to the input string value to convert
- `found`: JsonValueList pointer for collecting matching datetime values

## Dependencies
- Functions called/Symbols referenced:
  - [getScalar](../g/getScalar.md): Converts input to string scalar
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md): Converts C strings to PostgreSQL text
  - [parse_datetime](../p/parse_datetime.md): Core datetime parsing function with template support
  - [jspGetArg](../j/jspGetArg.md)/jspGetString/jspGetNumeric: Extract arguments from JSON path items
  - DirectFunctionCall1: Execute PostgreSQL type conversion functions
  - [checkTimezoneIsUsedForCast](../c/checkTimezoneIsUsedForCast.md): Validate timezone usage in conversions
  - Various datetime conversion functions (timestamp_date, timetz_time, etc.)
  - [anytime_typmod_check](../a/anytime_typmod_check.md)/anytimestamp_typmod_check: Validate time precision
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)/AdjustTimestampForTypmod: Apply precision to datetime values
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md): Calculate timezone offsets
  - [executeNextItem](executeNextItem.md): Continue JSON path evaluation
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](executeItemOptUnwrapTarget.md): Main item execution dispatcher
  - RETURN_ERROR: Error handling macro

## Notes and Other Information
- Returns JsonPathExecResult (jperOk on success, jperError on failure, jperNotFound if no format matches)
- Supports multiple ISO formats for automatic format detection when no template is provided
- .datetime() method accepts optional format template; other methods use predefined ISO formats
- Methods except .datetime() and .date() support optional time precision arguments
- Handles comprehensive type conversions between all PostgreSQL datetime types (date, time, timetz, timestamp, timestamptz)
- Manages timezone information separately in JsonbValue structure for proper JSON representation
- Caches compiled format templates in static array for performance optimization
- Validates input strings must be convertible to string scalars; non-string inputs cause errors
- Part of PostgreSQL's JSON path expression evaluation system for datetime operations

## Simplified Source

```c
static JsonPathExecResult
executeDateTimeMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
                      JsonbValue *jb, JsonValueList *found)
{
    JsonbValue jbvbuf;
    Datum value;
    text *datetime;
    Oid typid;
    int32 typmod = -1;
    int tz = 0;
    bool hasNext;
    JsonPathExecResult res = jperNotFound;
    JsonPathItem elem;
    int32 time_precision = -1;

    // Validate input is string
    if (!(jb = getScalar(jb, jbvString)))
        RETURN_ERROR(ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION),
                     errmsg("jsonpath item method .%s() can only be applied to a string",
                            jspOperationName(jsp->type)))));

    datetime = cstring_to_text_with_len(jb->val.string.val, jb->val.string.len);
    Oid collid = DEFAULT_COLLATION_OID;

    // Handle .datetime() with template vs other methods with ISO formats
    if (jsp->type == jpiDatetime && jsp->content.arg)
    {
        // Use custom template
        jspGetArg(jsp, &elem);
        if (elem.type != jpiString)
            elog(ERROR, "invalid jsonpath item type for .datetime() argument");

        text *template = cstring_to_text_with_len(jspGetString(&elem, &template_len),
                                                 template_len);
        value = parse_datetime(datetime, template, collid, true, &typid, &typmod, &tz,
                              jspThrowErrors(cxt) ? NULL : (Node *) &escontext);
        res = escontext.error_occurred ? jperError : jperOk;
    }
    else
    {
        // Try predefined ISO formats
        static const char *fmt_str[] = {
            "yyyy-mm-dd",                    // date
            "HH24:MI:SS.USTZ", "HH24:MI:SSTZ", "HH24:MI:SS.US", "HH24:MI:SS",  // time
            "yyyy-mm-dd HH24:MI:SS.USTZ", "yyyy-mm-dd HH24:MI:SSTZ",          // timestamptz
            "yyyy-mm-dd\"T\"HH24:MI:SS.USTZ", "yyyy-mm-dd\"T\"HH24:MI:SSTZ",
            "yyyy-mm-dd HH24:MI:SS.US", "yyyy-mm-dd HH24:MI:SS",              // timestamp
            "yyyy-mm-dd\"T\"HH24:MI:SS.US", "yyyy-mm-dd\"T\"HH24:MI:SS"
        };
        static text *fmt_txt[lengthof(fmt_str)] = {0};

        // Extract optional precision argument
        if (jsp->type != jpiDatetime && jsp->type != jpiDate && jsp->content.arg)
        {
            jspGetArg(jsp, &elem);
            time_precision = numeric_int4_opt_error(jspGetNumeric(&elem), &have_error);
            if (have_error)
                RETURN_ERROR(/* precision error */);
        }

        // Try each format until one works
        for (int i = 0; i < lengthof(fmt_str); i++)
        {
            if (!fmt_txt[i])
                fmt_txt[i] = cstring_to_text(fmt_str[i]);

            value = parse_datetime(datetime, fmt_txt[i], collid, true,
                                 &typid, &typmod, &tz, (Node *) &escontext);
            if (!escontext.error_occurred)
            {
                res = jperOk;
                break;
            }
        }
    }

    if (res == jperNotFound)
        RETURN_ERROR(/* format not recognized */);

    // Convert to target type based on method
    switch (jsp->type)
    {
        case jpiDate:
            // Convert to DATE
            switch (typid)
            {
                case TIMESTAMPOID:
                    value = DirectFunctionCall1(timestamp_date, value);
                    break;
                case TIMESTAMPTZOID:
                    checkTimezoneIsUsedForCast(cxt->useTz, "timestamptz", "date");
                    value = DirectFunctionCall1(timestamptz_date, value);
                    break;
            }
            typid = DATEOID;
            break;

        case jpiTime:
            // Convert to TIME and apply precision
            // ... similar conversion logic for other types
            break;

        // ... other type conversions
    }

    pfree(datetime);

    if (jperIsError(res))
        return res;

    hasNext = jspGetNext(jsp, &elem);
    if (!hasNext && !found)
        return res;

    jb = hasNext ? &jbvbuf : palloc(sizeof(*jb));
    jb->type = jbvDatetime;
    jb->val.datetime.value = value;
    jb->val.datetime.typid = typid;
    jb->val.datetime.typmod = typmod;
    jb->val.datetime.tz = tz;

    return executeNextItem(cxt, jsp, &elem, jb, found, hasNext);
}
```