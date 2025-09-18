# xml_send

## Location
src/backend/utils/adt/xml.c: 438 - 458

## Overview
Converts an XML value to its binary representation for transmission over the network using PostgreSQL's binary protocol.

## Definition


## Detailed Description
The xml_send function is a PostgreSQL type output function that serializes XML data into a binary format suitable for network transmission. This function is part of PostgreSQL's binary protocol support, allowing XML values to be efficiently sent from the server to client applications that use binary data transfer mode.

The function first converts the XML value to its string representation using xml_out_internal, then uses the PostgreSQL binary sending infrastructure (pq_sendtext) to properly encode the text data into a bytea format. The binary protocol handles character encoding conversion automatically during transmission.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro, where the first argument is the XML value to be sent

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_XML_P (retrieves XML argument)
  - xml_out_internal (converts XML to string representation)
  - pg_get_client_encoding (gets client character encoding)
  - pq_begintypsend (initializes binary send buffer)
  - pq_sendtext (sends text data in binary format)
  - pq_endtypsend (finalizes binary send buffer)
  - PG_RETURN_BYTEA_P (returns bytea result)
- Called from:
  - Binary protocol transmission system (no direct references found)

## Notes and Other Information
- This function is typically registered as the send function for the XML type in PostgreSQL's type system
- The function handles character encoding conversion through pq_sendtext rather than doing it explicitly
- The output is a bytea value containing the binary-encoded XML string
- Memory management is handled properly with pfree() to avoid leaks