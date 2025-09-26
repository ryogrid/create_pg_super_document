# message_level_is_interesting

## Location
[src/backend/utils/error/elog.c:276-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L276-L296)

## Overview
Determines whether an ereport/elog call with a given error level would actually produce any output, allowing callers to skip expensive preparatory work for messages that won't be processed.

## Definition
```c
bool message_level_is_interesting(int elevel)
```

## Detailed Description
This optimization function provides a fast way to determine if calling ereport() or elog() with a particular error level would result in any actual output. It consolidates the decision logic from errstart() to avoid duplicate expensive preparatory work when the message would ultimately be discarded. The function returns true if the message would either be an ERROR or higher severity (which are always processed), or if the message would be sent to either the server log or the client based on current logging and client message settings. This is particularly useful for expensive operations like string formatting, translation, or complex data gathering that should only be performed if the resulting message will actually be output.

## Parameters / Member Variables
- `elevel`: The error/message level to check (e.g., DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC, LOG)

## Dependencies
- Functions called/Symbols referenced:
  - [should_output_to_server](../s/should_output_to_server.md)
  - [should_output_to_client](../s/should_output_to_client.md)
  - ERROR (constant)
- Called from (representative examples):
  - [ShowTransactionState](../S/ShowTransactionState.md)
  - [log_invalid_page](../l/log_invalid_page.md)
  - [forget_invalid_pages](../f/forget_invalid_pages.md)
  - [forget_invalid_pages_db](../f/forget_invalid_pages_db.md)
  - [reportDependentObjects](../r/reportDependentObjects.md)
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md)
  - [ProcessWalSndrMessage](../P/ProcessWalSndrMessage.md)
  - [ProcessStandbyReplyMessage](../P/ProcessStandbyReplyMessage.md)
  - [ProcessStandbyHSFeedbackMessage](../P/ProcessStandbyHSFeedbackMessage.md)
  - [ProcSleep](../P/ProcSleep.md)

## Notes and Other Information
- This function must be kept in sync with the decision-making logic in errstart() to ensure consistency
- Designed for performance optimization - allows callers to avoid expensive preparatory work for messages that won't be output
- Not useful to call immediately before a bare ereport/elog call since those functions perform the same checks internally
- ERROR and higher severity messages always return true since they are always processed regardless of logging settings
- The function is declared without static/inline keywords, making it available to other compilation units
- Common usage pattern: check this function before performing expensive string operations, translations, or data gathering for log messages