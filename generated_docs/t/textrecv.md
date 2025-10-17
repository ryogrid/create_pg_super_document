# textrecv

## Location
[src/backend/utils/adt/varlena.c:601-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L601-L618)

## Overview
The  function converts external binary format data to PostgreSQL's internal text representation, serving as the binary receive function for the text data type.

## Definition

```c
Datum
textrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL data type binary receive function that handles the conversion from external binary protocol format to PostgreSQL's internal text format. It reads binary data from a StringInfo buffer using the PostgreSQL message protocol, extracts the string data, and converts it to a text datum. This function is part of PostgreSQL's binary protocol support and is used when text values are transmitted in binary format between client and server or in replication streams.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: A  pointer containing the binary data buffer to be processed

## Dependencies
- Functions called/Symbols referenced:
  - : Protocol function that extracts a text string from the message buffer
  - : Utility function that converts a C string with known length to text format
  - : Macro for returning a text pointer from a PostgreSQL function
  - : Memory deallocation function to free the temporary string
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is registered as the binary receive function for the  data type in PostgreSQL's type system
- It uses PostgreSQL's message protocol functions to safely extract binary data
- Memory management is handled properly with  to avoid memory leaks
- The function extracts the remaining bytes from the buffer ()
- Complementary to the  function, forming the binary send/receive pair for text data type
- Located in src/backend/utils/adt/varlena.c

## Simplified Source

```c
Datum textrecv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);

    // Extract text string from binary protocol buffer
    int nbytes;
    char *str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

    // Convert C string to text with known length
    text *result = cstring_to_text_with_len(str, nbytes);

    // Clean up temporary string
    pfree(str);

    PG_RETURN_TEXT_P(result);
}
``` 