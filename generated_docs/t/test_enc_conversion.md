# test_enc_conversion

## Location
[src/test/regress/regress.c:1174-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L1174-L1290)

## Overview
A PostgreSQL regression test function that performs character encoding conversion between different encodings and returns both the number of successfully converted bytes and the converted result.

## Definition

```c
structure.
			 */
			Assert(oklen < srclen);
```
## Detailed Description
The  function is a PostgreSQL test utility that converts a byte string from one character encoding to another. It accepts a bytea input string, source encoding name, destination encoding name, and a boolean flag indicating whether to suppress errors. The function returns a composite type containing the number of bytes successfully converted and the converted bytea result.

The function handles two main scenarios:
1. **Same encoding conversion**: When source and destination encodings are identical, it validates the input string and returns it unchanged if valid
2. **Cross-encoding conversion**: Uses PostgreSQL's encoding conversion system to transform the string from source to destination encoding

The function includes comprehensive error handling, memory management, and supports both strict and lenient conversion modes based on the  parameter.

## Parameters / Member Variables
-  (bytea): The input byte string to be converted
-  (Name): The name of the source character encoding
-  (Name): The name of the destination character encoding  
-  (bool): If true, suppresses errors and returns partial results for invalid input

## Dependencies
- Functions called/Symbols referenced:
  - : Extract bytea parameter
  - : Extract Name parameter
  - : Extract boolean parameter
  - : Convert encoding name to encoding ID
  - : Verify multibyte string validity
  - : Find conversion function between encodings
  - : Perform actual encoding conversion
  - : Create return tuple
  - : Return result datum
- Called from (representative examples):
  - : Test setup function (src/test/regress/regress.c:1172)

## Notes and Other Information
- Located in the regression test suite ()
- Returns a composite type with two fields: converted byte count and converted bytea
- Handles memory allocation carefully to prevent overflow during conversion
- Uses  constant to estimate maximum output size
- Validates encoding names and reports appropriate errors for invalid encodings
- Supports partial conversion when  is true, returning valid prefix of input

## Simplified Source

```c
Datum
test_enc_conversion(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    bytea *string = PG_GETARG_BYTEA_PP(0);
    char *src_encoding_name = NameStr(*PG_GETARG_NAME(1));
    char *dest_encoding_name = NameStr(*PG_GETARG_NAME(2));
    bool noError = PG_GETARG_BOOL(3);

    // Convert encoding names to IDs
    int src_encoding = pg_char_to_encoding(src_encoding_name);
    int dest_encoding = pg_char_to_encoding(dest_encoding_name);

    // Validate encoding names
    if (src_encoding < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid source encoding name \"%s\"", src_encoding_name)));
    if (dest_encoding < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid destination encoding name \"%s\"", dest_encoding_name)));

    // Setup tuple descriptor for return type
    TupleDesc tupdesc;
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    tupdesc = BlessTupleDesc(tupdesc);

    // Extract source string data
    Size srclen = VARSIZE_ANY_EXHDR(string);
    char *src = VARDATA_ANY(string);

    bytea *retval;
    int convertedbytes;

    if (src_encoding == dest_encoding)
    {
        // Same encoding: just validate the string
        int oklen = pg_encoding_verifymbstr(src_encoding, src, srclen);

        if (oklen == srclen)
        {
            // String is fully valid
            convertedbytes = oklen;
            retval = string;
        }
        else if (!noError)
        {
            // Report invalid encoding error
            report_invalid_encoding(src_encoding, src + oklen, srclen - oklen);
        }
        else
        {
            // Return valid prefix only
            convertedbytes = oklen;
            retval = (bytea *) palloc(oklen + VARHDRSZ);
            SET_VARSIZE(retval, oklen + VARHDRSZ);
            memcpy(VARDATA(retval), src, oklen);
        }
    }
    else
    {
        // Different encodings: perform conversion
        Oid proc = FindDefaultConversionProc(src_encoding, dest_encoding);
        if (!OidIsValid(proc))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("default conversion function for encoding \"%s\" to \"%s\" does not exist",
                                  pg_encoding_to_char(src_encoding), pg_encoding_to_char(dest_encoding))));

        // Check for memory overflow
        if (srclen >= (MaxAllocSize / (Size) MAX_CONVERSION_GROWTH))
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                           errmsg("out of memory")));

        // Allocate destination buffer
        Size dstsize = (Size) srclen * MAX_CONVERSION_GROWTH + 1;
        char *dst = MemoryContextAlloc(CurrentMemoryContext, dstsize);

        // Perform the conversion
        convertedbytes = pg_do_encoding_conversion_buf(proc, src_encoding, dest_encoding,
                                                       (unsigned char *) src, srclen,
                                                       (unsigned char *) dst, dstsize, noError);
        int dstlen = strlen(dst);

        // Build result bytea
        retval = (bytea *) palloc(dstlen + VARHDRSZ);
        SET_VARSIZE(retval, dstlen + VARHDRSZ);
        memcpy(VARDATA(retval), dst, dstlen);

        pfree(dst);
    }

    // Return tuple with (converted_bytes, converted_string)
    Datum values[2] = {Int32GetDatum(convertedbytes), PointerGetDatum(retval)};
    bool nulls[2] = {false, false};
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
```