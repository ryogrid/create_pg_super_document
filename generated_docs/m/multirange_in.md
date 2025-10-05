# multirange_in

## Location
[src/backend/utils/adt/multirangetypes.c:117-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L117-L298)

## Overview
Converts a string representation to a PostgreSQL multirange value, parsing curly bracket-delimited lists of ranges separated by commas.

## Definition

```c
Datum
multirange_in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the input function for PostgreSQL multirange types, responsible for converting text representations into internal multirange format. It expects input in the format  where:

- The entire multirange is bounded by curly braces 
- Individual ranges are separated by commas
- Empty multiranges are represented as 
- Each range follows standard range syntax  or 
- Empty ranges are represented with the literal "empty"
- Whitespace is accepted around braces and commas

The function implements a comprehensive state machine parser that handles:
- Quoted strings within range bounds
- Backslash escaping (both inside and outside quotes)  
- Double-quote escaping within quoted strings
- Proper validation of multirange syntax

The parser delegates individual range parsing to the underlying range type's input function while handling the multirange-specific syntax and structure.

## Parameters / Member Variables
- : String representation of the multirange to parse
- : OID of the multirange type being created
- : Type modifier for the multirange type
- : Error context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [get_multirange_io_data](../g/get_multirange_io_data.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pnstrdup](../p/pnstrdup.md)
  - [repalloc](../r/repalloc.md)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - RangeIsEmpty
  - [make_multirange](make_multirange.md)
  - PG_RETURN_MULTIRANGE_P
- Called from:
  - PostgreSQL type system (input function registration)

## Notes and Other Information
- Uses a finite state machine with states: BEFORE_RANGE, IN_RANGE, AFTER_RANGE, IN_RANGE_QUOTED, IN_RANGE_ESCAPED, IN_RANGE_QUOTED_ESCAPED, FINISHED
- Empty ranges within the multirange are filtered out during construction
- Supports soft error reporting through error context
- Memory management uses palloc/repalloc for dynamic range array allocation
- Initial capacity for ranges is 8, doubled when exceeded
- Comprehensive error messages provide specific details about parsing failures

## Simplified Source

```c
Datum
multirange_in(PG_FUNCTION_ARGS)
{
    char *input_str = PG_GETARG_CSTRING(0);
    Oid mltrngtypoid = PG_GETARG_OID(1);
    Oid typmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;

    // Initialize parsing state
    MultirangeIOData *cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_input);
    TypeCacheEntry *rangetyp = cache->typcache->rngtype;
    int32 range_count = 0;
    int32 range_capacity = 8;
    RangeType **ranges = palloc(range_capacity * sizeof(RangeType *));

    const char *ptr = input_str;
    MultirangeParseState parse_state = MULTIRANGE_BEFORE_RANGE;

    // Skip initial whitespace and expect opening brace
    while (*ptr && isspace(*ptr)) ptr++;
    if (*ptr != '{') {
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("malformed multirange literal: \"%s\"", input_str),
                 errdetail("Missing left brace.")));
    }
    ptr++;

    // Parse ranges using state machine
    const char *range_str_begin = NULL;
    for (; parse_state != MULTIRANGE_FINISHED; ptr++) {
        char ch = *ptr;

        if (ch == '\0') {
            ereturn(escontext, (Datum) 0,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("malformed multirange literal: \"%s\"", input_str)));
        }

        if (isspace(ch)) continue;

        switch (parse_state) {
            case MULTIRANGE_BEFORE_RANGE:
                if (ch == '[' || ch == '(') {
                    range_str_begin = ptr;
                    parse_state = MULTIRANGE_IN_RANGE;
                } else if (ch == '}') {
                    parse_state = MULTIRANGE_FINISHED;
                } else if (pg_strncasecmp(ptr, RANGE_EMPTY_LITERAL,
                                        strlen(RANGE_EMPTY_LITERAL)) == 0) {
                    // Skip empty range
                    ptr += strlen(RANGE_EMPTY_LITERAL) - 1;
                    parse_state = MULTIRANGE_AFTER_RANGE;
                }
                break;

            case MULTIRANGE_IN_RANGE:
                if (ch == ']' || ch == ')') {
                    // Extract and parse range
                    int32 range_str_len = ptr - range_str_begin + 1;
                    char *range_str = pnstrdup(range_str_begin, range_str_len);

                    // Expand array if needed
                    if (range_capacity == range_count) {
                        range_capacity *= 2;
                        ranges = repalloc(ranges, range_capacity * sizeof(RangeType *));
                    }

                    // Parse individual range
                    Datum range_datum;
                    if (!InputFunctionCallSafe(&cache->typioproc, range_str,
                                             cache->typioparam, typmod,
                                             escontext, &range_datum)) {
                        PG_RETURN_NULL();
                    }

                    RangeType *range = DatumGetRangeTypeP(range_datum);
                    if (!RangeIsEmpty(range)) {
                        ranges[range_count++] = range;
                    }
                    parse_state = MULTIRANGE_AFTER_RANGE;
                } else if (ch == '"') {
                    parse_state = MULTIRANGE_IN_RANGE_QUOTED;
                } else if (ch == '\\') {
                    parse_state = MULTIRANGE_IN_RANGE_ESCAPED;
                }
                break;

            case MULTIRANGE_AFTER_RANGE:
                if (ch == ',') {
                    parse_state = MULTIRANGE_BEFORE_RANGE;
                } else if (ch == '}') {
                    parse_state = MULTIRANGE_FINISHED;
                }
                break;

            // Handle quoted and escaped states
            case MULTIRANGE_IN_RANGE_ESCAPED:
                parse_state = MULTIRANGE_IN_RANGE;
                break;
            case MULTIRANGE_IN_RANGE_QUOTED:
                if (ch == '"') {
                    if (*(ptr + 1) == '"') {
                        ptr++; // Skip escaped quote
                    } else {
                        parse_state = MULTIRANGE_IN_RANGE;
                    }
                } else if (ch == '\\') {
                    parse_state = MULTIRANGE_IN_RANGE_QUOTED_ESCAPED;
                }
                break;
            case MULTIRANGE_IN_RANGE_QUOTED_ESCAPED:
                parse_state = MULTIRANGE_IN_RANGE_QUOTED;
                break;
        }
    }

    // Skip trailing whitespace
    while (*ptr && isspace(*ptr)) ptr++;
    if (*ptr != '\0') {
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("malformed multirange literal: \"%s\"", input_str),
                 errdetail("Junk after closing right brace.")));
    }

    // Create and return multirange
    MultirangeType *ret = make_multirange(mltrngtypoid, rangetyp, range_count, ranges);
    PG_RETURN_MULTIRANGE_P(ret);
}
```