# chopStringInfoNewlines

## Location
src/backend/utils/adt/xml.c: 2313 - 2323

## Overview
A static utility function that removes all trailing newline characters from a StringInfo string, ensuring clean string formatting by eliminating unwanted line breaks at the end.

## Definition

```c
static void
chopStringInfoNewlines(StringInfo str)
```
## Detailed Description
This function performs a simple but important string cleanup operation by iteratively removing newline characters ('\n') from the end of a StringInfo buffer. It works by scanning backwards from the end of the string, replacing each trailing newline with a null terminator and decrementing the string length accordingly. The operation continues until either the string becomes empty or a non-newline character is encountered.

The function is particularly useful in XML processing contexts where formatted output may accumulate unwanted trailing newlines that need to be cleaned up before final presentation.

## Parameters / Member Variables
- : A StringInfo pointer containing the string to be processed. The string is modified in-place, with trailing newlines removed and the length adjusted accordingly.

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - operates directly on StringInfo data and len fields)
- Called from (representative examples):
  - xml_errorHandler
  - appendStringInfoLineSeparator
  - PgXmlErrorContext

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xml.c compilation unit
- The function modifies the StringInfo in-place, making it an efficient operation with no memory allocation
- The while loop ensures all consecutive trailing newlines are removed, not just a single one
- The function is safe to call on empty strings (len == 0 condition prevents accessing invalid memory)
- Part of PostgreSQL's XML processing infrastructure for maintaining clean formatted output