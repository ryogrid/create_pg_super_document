# NoticeProcessor

## Location
src/bin/psql/common.c: 267 - 296

## Overview
NoticeProcessor is a callback function used in psql to handle backend notice messages (INFO, WARNING, etc.) from PostgreSQL server connections.

## Definition


## Detailed Description
NoticeProcessor serves as a notice message handler for PostgreSQL client connections in psql. When the PostgreSQL backend sends notice messages such as INFO, WARNING, or other informational messages, this function is called to process and display them. The function takes the raw message from the backend and outputs it using the standard psql logging mechanism via pg_log_info(). This ensures that notice messages are properly formatted and displayed to the user in a consistent manner with other psql output.

## Parameters / Member Variables
- : A void pointer argument that is not used in the current implementation (marked as unused to avoid compiler warnings)
- : A const char pointer containing the notice message text received from the PostgreSQL backend

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info (for outputting the notice message)
  - sigjmp_buf (referenced in related error handling context)
- Called from (representative examples):
  - do_connect (when setting up connection notice processors)
  - Used as callback in PostgreSQL connection setup

## Notes and Other Information
- The function follows the standard PostgreSQL notice processor callback signature
- The arg parameter is explicitly marked as unused, indicating this implementation doesn't require additional context
- This is part of psql's client-side message handling infrastructure
- The function ensures notice messages are displayed to users in a consistent format alongside other psql output