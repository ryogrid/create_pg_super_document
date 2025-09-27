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
  - [pq_beginmessage](../p/pq_beginmessage.md)
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

## Simplified Source

```c
// Simplified version of send_message_to_frontend
static void send_message_to_frontend(ErrorData *edata) {
    StringInfoData msgbuf;

    // Check protocol version - modern (3.0+) vs legacy
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3 || FrontendProtocol == 0) {

        // Modern protocol: Send structured message with separate fields
        const char *severity = error_severity(edata->elevel);

        // Begin message - Notice for warnings, Error for errors
        if (edata->elevel < ERROR)
            pq_beginmessage(&msgbuf, PqMsg_NoticeResponse);
        else
            pq_beginmessage(&msgbuf, PqMsg_ErrorResponse);

        // Send severity (both localized and non-localized)
        pq_sendbyte(&msgbuf, PG_DIAG_SEVERITY);
        err_sendstring(&msgbuf, _(severity));
        pq_sendbyte(&msgbuf, PG_DIAG_SEVERITY_NONLOCALIZED);
        err_sendstring(&msgbuf, severity);

        // Send SQL state code
        pq_sendbyte(&msgbuf, PG_DIAG_SQLSTATE);
        err_sendstring(&msgbuf, unpack_sql_state(edata->sqlerrcode));

        // Send primary message (required)
        pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
        err_sendstring(&msgbuf, edata->message ? edata->message : _("missing error text"));

        // Send optional diagnostic fields if present
        send_optional_field(&msgbuf, PG_DIAG_MESSAGE_DETAIL, edata->detail);
        send_optional_field(&msgbuf, PG_DIAG_MESSAGE_HINT, edata->hint);
        send_optional_field(&msgbuf, PG_DIAG_CONTEXT, edata->context);

        // Send database object names if present
        send_optional_field(&msgbuf, PG_DIAG_SCHEMA_NAME, edata->schema_name);
        send_optional_field(&msgbuf, PG_DIAG_TABLE_NAME, edata->table_name);
        send_optional_field(&msgbuf, PG_DIAG_COLUMN_NAME, edata->column_name);
        send_optional_field(&msgbuf, PG_DIAG_DATATYPE_NAME, edata->datatype_name);
        send_optional_field(&msgbuf, PG_DIAG_CONSTRAINT_NAME, edata->constraint_name);

        // Send position information if present
        send_optional_position(&msgbuf, PG_DIAG_STATEMENT_POSITION, edata->cursorpos);
        send_optional_position(&msgbuf, PG_DIAG_INTERNAL_POSITION, edata->internalpos);
        send_optional_field(&msgbuf, PG_DIAG_INTERNAL_QUERY, edata->internalquery);

        // Send source location information if present
        send_optional_field(&msgbuf, PG_DIAG_SOURCE_FILE, edata->filename);
        send_optional_position(&msgbuf, PG_DIAG_SOURCE_LINE, edata->lineno);
        send_optional_field(&msgbuf, PG_DIAG_SOURCE_FUNCTION, edata->funcname);

        // Terminate and send message
        pq_sendbyte(&msgbuf, '\0');
        pq_endmessage(&msgbuf);

    } else {
        // Legacy protocol: Send simple text message
        StringInfoData legacy_buf;
        initStringInfo(&legacy_buf);

        appendStringInfo(&legacy_buf, "%s:  ", _(error_severity(edata->elevel)));
        appendStringInfoString(&legacy_buf, edata->message ? edata->message : _("missing error text"));
        appendStringInfoChar(&legacy_buf, '\n');

        pq_putmessage_v2((edata->elevel < ERROR) ? 'N' : 'E', legacy_buf.data, legacy_buf.len + 1);
        pfree(legacy_buf.data);
    }

    // Flush to ensure client receives message immediately
    pq_flush();
}

// Helper function for optional string fields (conceptual - not in actual code)
static void send_optional_field(StringInfoData *msgbuf, char field_type, const char *value) {
    if (value) {
        pq_sendbyte(msgbuf, field_type);
        err_sendstring(msgbuf, value);
    }
}

// Helper function for optional position fields (conceptual - not in actual code)
static void send_optional_position(StringInfoData *msgbuf, char field_type, int position) {
    if (position > 0) {
        char buf[12];
        snprintf(buf, sizeof(buf), "%d", position);
        pq_sendbyte(msgbuf, field_type);
        err_sendstring(msgbuf, buf);
    }
}
```

Key simplifications made:
- Consolidated repetitive optional field sending into conceptual helper functions
- Grouped related fields together logically (severity, message, object names, positions, source info)
- Removed detailed comments within the protocol logic to focus on structure
- Simplified variable declarations and buffer handling
- Maintained the core two-path logic (modern vs legacy protocol)
- Preserved all essential functionality while improving readability