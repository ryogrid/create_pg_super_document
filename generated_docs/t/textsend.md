# textsend

## Location
[src/backend/utils/adt/varlena.c:619-633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L619-L633)

## Overview
The  function converts PostgreSQL's internal text representation to external binary format, serving as the binary send function for the text data type.

## Definition

```c
Datum
textsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL data type binary send function that handles the conversion from PostgreSQL's internal text format to external binary protocol format. It takes a text datum as input, extracts the string data using PostgreSQL's variable-length data macros, and packages it into a binary message using the PostgreSQL protocol functions. This function is part of PostgreSQL's binary protocol support and is used when text values need to be transmitted in binary format between client and server or in replication streams.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: A text datum (using  for potentially packed text) to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - : Protocol function that initializes a binary send buffer
  - : Protocol function that writes text data to the binary buffer
  - : Protocol function that finalizes the binary buffer and returns bytea
  - : Macro for returning a bytea pointer from a PostgreSQL function
  - : Macro to get the data portion of a variable-length datum
  - : Macro to get the size excluding the header of a variable-length datum
- Called from (representative examples):
  - : Used in node tree binary send processing
  - : Used in CHAR(n) data type binary send
  - : Used in VARCHAR data type binary send

## Notes and Other Information
- This function is registered as the binary send function for the  data type in PostgreSQL's type system
- It uses PostgreSQL's message protocol functions to safely create binary data
- The function handles variable-length data correctly using PostgreSQL's VARDATA and VARSIZE macros
- Uses  which can handle both normal and packed text representations efficiently
- Complementary to the  function, forming the binary send/receive pair for text data type
- The result is a bytea that contains the binary representation suitable for network transmission
- Located in 