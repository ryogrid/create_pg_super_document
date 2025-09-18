# DoState

## Location
[src/test/modules/test_json_parser/test_json_parser_incremental.c:42-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_json_parser/test_json_parser_incremental.c#L42-L47)

## Overview
DoState is a state structure used by the incremental JSON parser test module to maintain parsing context and formatting state during semantic processing of JSON documents.

## Definition


## Detailed Description
The DoState structure serves as a container for state information passed to JSON semantic action functions during incremental JSON parsing. It is used specifically in the test_json_parser_incremental test module to maintain context across different parsing events (object start/end, array start/end, field processing, etc.). The structure enables the parser to format JSON output correctly by tracking element positioning and providing a reusable buffer for string processing.

This structure is allocated when the '-s' flag is passed to the test program, enabling semantic processing mode. It is passed as the void *state parameter to all semantic action functions defined in the JsonSemAction structure.

## Parameters / Member Variables
- : Pointer to the JsonLexContext used for JSON lexical analysis and parsing
- : Boolean flag indicating whether the current element is the first in its container (object or array), used for proper comma placement in formatted output
- : StringInfo buffer used for temporary string processing, particularly for escaping JSON strings during output formatting

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - StringInfo
- Called from (representative examples):
  - [main](../m/main.md) (for allocation and initialization)
  - [do_object_start](../d/do_object_start.md)
  - [do_object_end](../d/do_object_end.md)
  - [do_object_field_start](../d/do_object_field_start.md)
  - [do_array_start](../d/do_array_start.md)
  - [do_array_end](../d/do_array_end.md)
  - [do_array_element_start](../d/do_array_element_start.md)
  - [do_scalar](../d/do_scalar.md)

## Notes and Other Information
- This structure is specific to the test_json_parser_incremental test module and is not part of the core PostgreSQL JSON parsing infrastructure
- The structure is allocated using palloc() when semantic processing is enabled via the '-s' command line flag
- The elem_is_first flag is crucial for generating properly formatted JSON output with correct comma placement between elements
- The buf member is used primarily in the do_scalar function for escaping JSON string values before output
- Located in src/test/modules/test_json_parser/test_json_parser_incremental.c:42-47