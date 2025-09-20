# dsimple_init

## Location
[src/backend/tsearch/dict_simple.c:30-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_simple.c#L30-L74)

## Overview
Initializes a simple dictionary for PostgreSQL's text search system by parsing configuration options and creating a DictSimple structure.

## Definition

```c
Datum
dsimple_init(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the initialization routine for PostgreSQL's simple dictionary text search feature. It processes a list of dictionary options provided during dictionary creation and configures a DictSimple structure accordingly. The function supports two main configuration parameters: "stopwords" for specifying a list of words to ignore during text search, and "accept" for controlling whether unrecognized words should be accepted or rejected. The function implements proper error handling to prevent duplicate parameter specifications and validates parameter names to ensure only recognized options are accepted.

## Parameters / Member Variables
- : A List pointer containing DefElem structures with configuration parameters for the dictionary

## Dependencies
- Functions called/Symbols referenced:
  - DictSimple (structure type)
  - [DefElem](../D/DefElem.md) (structure type for parameter definitions)
  - [defGetString](defGetString.md) (extracts string value from DefElem)
  - [defGetBoolean](defGetBoolean.md) (extracts boolean value from DefElem)
  - [readstoplist](../r/readstoplist.md) (loads stopword list from file)
  - [lowerstr](../l/lowerstr.md) (function for lowercasing strings)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - ereport (PostgreSQL error reporting)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager)

## Notes and Other Information
- The function sets  as the default behavior for handling unrecognized words
- Implements validation to prevent multiple specifications of the same parameter (stopwords or accept)
- Uses PostgreSQL's error reporting system with specific error codes for invalid parameters
- Part of the text search dictionary framework in PostgreSQL
- Located in src/backend/tsearch/dict_simple.c:30-74