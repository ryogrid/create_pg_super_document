# add_indent

## Location
[src/backend/utils/adt/jsonb.c:615-637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L615-L637)

## Overview
A utility function that adds indentation formatting to a StringInfo buffer when pretty-printing JSON output.

## Definition

```c
static void
add_indent(StringInfo out, bool indent, int level)
```
## Detailed Description
The  function is a simple formatting utility used during JSON serialization to add proper indentation for pretty-printed output. When the  parameter is true, it adds a newline character followed by the appropriate number of spaces based on the specified indentation level. Each indentation level corresponds to 4 spaces. This function is crucial for generating human-readable JSON output with proper formatting.

## Parameters / Member Variables
- : StringInfo buffer to append the indentation formatting to
- : Boolean flag indicating whether indentation should be applied 
- : Integer specifying the indentation depth (multiplied by 4 to get actual space count)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
  - appendStringInfoSpaces
- Called from (representative examples):
  - JsonbToCStringWorker (multiple locations in jsonb.c:530, 543, 554, 584, 591, 598)

## Notes and Other Information
- This is a static function used internally within jsonb.c for JSON formatting
- Uses a fixed indentation of 4 spaces per level, which is a common convention for JSON pretty-printing
- Only adds formatting when the indent flag is true, allowing the same code path to handle both compact and pretty-printed output
- The function is lightweight and focused solely on formatting, with no error handling needed due to its simple nature