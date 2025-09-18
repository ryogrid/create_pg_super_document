# send_message_to_frontend

## Location
[src/backend/utils/error/elog.c:3489-3666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L3489-L3666)

## Overview
A static function responsible for formatting and transmitting error/notice messages to the PostgreSQL client using the appropriate frontend-backend protocol format.

## Definition
```c
static void send_message_to_frontend(ErrorData *edata)
```

## Detailed Description
This function serves as the central dispatcher for sending error and notice messages to PostgreSQL clients. It handles both modern protocol version 3.0+ and legacy protocol formats. For modern protocols, it constructs structured messages with separate diagnostic fields including severity, SQL state, primary message, detail, hint, context, and various source location information. Each field is properly encoded and transmitted using the err_sendstring function for safe string handling. For older protocols, it creates a backwards-compatible plain text message format. The function ensures that critical error information reaches the client even during error recursion scenarios by utilizing the robust err_sendstring mechanism.

## Parameters / Member Variables
- `edata`: Pointer to ErrorData structure containing all error/notice information including severity level, message text, SQL state, context information, and source location details

## Dependencies
- Functions called/Symbols referenced:
  - PG_PROTOCOL_MAJOR
  - pq_beginmessage
  - PqMsg_NoticeResponse, PqMsg_ErrorResponse
  - [error_severity](../e/error_severity.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [err_sendstring](../e/err_sendstring.md) (extensively used for all string fields)
  - [unpack_sql_state](../u/unpack_sql_state.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - [pq_putmessage_v2](../p/pq_putmessage_v2.md)
  - pq_flush
- Called from (representative examples):
  - [EmitErrorReport](../E/EmitErrorReport.md)

## Notes and Other Information
This function is critical to PostgreSQL's error handling architecture and maintains compatibility across different protocol versions. It extensively uses err_sendstring to ensure reliable string transmission even during error recursion scenarios. The function always flushes output to ensure clients receive error messages promptly, even if the backend encounters fatal errors. The structured message format in protocol 3.0+ allows clients to extract and display specific diagnostic information appropriately.