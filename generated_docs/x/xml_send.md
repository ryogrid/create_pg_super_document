# xml_send

## Location
[src/backend/utils/adt/xml.c:438-458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L438-L458)

## Overview
Converts an XML value to its binary representation for transmission over the network using PostgreSQL's binary protocol.

## Definition

```c
Datum
xml_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The xml_send function is a PostgreSQL type output function that serializes XML data into a binary format suitable for network transmission. This function is part of PostgreSQL's binary protocol support, allowing XML values to be efficiently sent from the server to client applications that use binary data transfer mode.

The function first converts the XML value to its string representation using xml_out_internal, then uses the PostgreSQL binary sending infrastructure (pq_sendtext) to properly encode the text data into a bytea format. The binary protocol handles character encoding conversion automatically during transmission.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments macro, where the first argument is the XML value to be sent
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_XML_P (retrieves XML argument)
  - [xml_out_internal](xml_out_internal.md) (converts XML to string representation)
  - [pg_get_client_encoding](../p/pg_get_client_encoding.md) (gets client character encoding)
  - [pq_begintypsend](../p/pq_begintypsend.md) (initializes binary send buffer)
  - [pq_sendtext](../p/pq_sendtext.md) (sends text data in binary format)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalizes binary send buffer)
  - PG_RETURN_BYTEA_P (returns bytea result)
- Called from:
  - Binary protocol transmission system (no direct references found)

## Notes and Other Information
- This function is typically registered as the send function for the XML type in PostgreSQL's type system
- The function handles character encoding conversion through pq_sendtext rather than doing it explicitly
- The output is a bytea value containing the binary-encoded XML string
- Memory management is handled properly with pfree() to avoid leaks

## Simplified Source

```c
Datum xml_send(PG_FUNCTION_ARGS) {
    xmltype *x = PG_GETARG_XML_P(0);
    StringInfoData buf;

    // Convert XML to string with client encoding declaration
    char *outval = xml_out_internal(x, pg_get_client_encoding());

    // Create binary protocol message
    pq_begintypsend(&buf);
    pq_sendtext(&buf, outval, strlen(outval));
    pfree(outval);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```