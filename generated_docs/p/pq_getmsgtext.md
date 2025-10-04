# pq_getmsgtext

## Location
[src/backend/libpq/pqformat.c:546-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L546-L578)

## Overview
Extracts a counted text string from a message buffer with optional character encoding conversion, always returning a freshly allocated null-terminated string.

## Definition

```c
char *
pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
```
## Detailed Description
The  function retrieves a text string of specified length from a PostgreSQL message buffer and performs character encoding conversion from client encoding to server encoding if necessary. It always returns a pointer to a freshly 'd result that is null-terminated. The function also returns the actual byte length of the converted string through the  output parameter. If no conversion is needed, it creates a copy of the original data with a null terminator added.

## Parameters / Member Variables
- `msg`: A  structure representing the message buffer being read from
- `rawbytes`: The number of raw bytes to extract from the message buffer (must be non-negative)
- `*nbytes`: Output parameter that receives the actual byte length of the converted/copied string
## Dependencies
- Functions called/Symbols referenced:
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
  -  (character encoding conversion function)
  -  (PostgreSQL memory allocation)
  -  (memory copy function)
  -  (string length function)
- Called from (representative examples):
  -  (enum type receive function)
  -  (JSON type receive function)
  -  (JSONB type receive function)
  -  (JSON path type receive function)
  -  (name type receive function)
  -  (C string type receive function)
  -  (blank-padded char type receive function)
  -  (varchar type receive function)
  -  (text type receive function)
  -  (unknown type receive function)

## Notes and Other Information
- Always returns a freshly allocated string using , requiring the caller to manage memory
- Automatically adds null termination to ensure the result is a valid C string
- Performs character encoding conversion from client to server encoding when necessary
- Returns the actual converted string length through the  parameter, which may differ from  after conversion
- Validates data availability before processing to prevent buffer overruns
- Advances the message cursor automatically to maintain proper position tracking
- Commonly used for receiving text-based data types in PostgreSQL protocol messages
- The returned string length in  reflects the post-conversion size, not the original raw bytes

## Simplified Source

```c
char *pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes) {
    char *str;
    char *p;

    // Validate we have enough data in the message buffer
    if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor))
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("insufficient data left in message")));

    // Get pointer to the data and advance cursor
    str = &msg->data[msg->cursor];
    msg->cursor += rawbytes;

    // Convert from client encoding to server encoding
    p = pg_client_to_server(str, rawbytes);

    if (p != str) {
        // Conversion happened - use converted string
        *nbytes = strlen(p);
        return p;  // pg_client_to_server already allocated memory
    } else {
        // No conversion needed - make a copy
        p = palloc(rawbytes + 1);
        memcpy(p, str, rawbytes);
        p[rawbytes] = '\0';  // Null-terminate
        *nbytes = rawbytes;
        return p;
    }
}
```