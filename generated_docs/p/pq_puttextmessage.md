# pq_puttextmessage

## Location
src/backend/libpq/pqformat.c: 367 - 387

## Overview
Generates and sends a character set-converted message to the client in a single step, handling encoding conversion automatically.

## Definition


## Detailed Description
The  function provides a convenient one-step method for sending text messages to PostgreSQL clients with automatic character encoding conversion. It combines the functionality of character set conversion and message transmission, similar to  but specifically designed for null-terminated strings that may require encoding conversion from the server's internal encoding to the client's expected encoding.

The function first attempts to convert the string from the server encoding to the client encoding using . If conversion occurs (indicated by the returned pointer being different from the input), it sends the converted string and frees the conversion buffer. If no conversion is needed, it sends the original string directly.

## Parameters / Member Variables
- : The message type character that identifies the type of message being sent
- : A null-terminated string containing the message content to be sent

## Dependencies
- Functions called/Symbols referenced:
  - strlen (to determine string length)
  - pg_server_to_client (for character encoding conversion)
  - pq_putmessage (for actual message transmission)
  - pfree (to free converted string buffer when needed)
- Called from (representative examples):
  - bbsink_copystream_begin_backup (src/backend/backup/basebackup_copy.c:155)
  - SendXlogRecPtrResult (src/backend/backup/basebackup_copy.c:371)

## Notes and Other Information
- Automatically handles character set conversion between server and client encodings
- More convenient than manually calling pg_server_to_client followed by pq_putmessage
- Properly manages memory by freeing converted strings when necessary
- The message includes the null terminator in the transmitted data (slen + 1)
- Part of PostgreSQL's client-server communication infrastructure
- Particularly useful for sending textual status messages and notifications that need proper encoding
- Optimizes the common case where no encoding conversion is needed by avoiding unnecessary memory allocation