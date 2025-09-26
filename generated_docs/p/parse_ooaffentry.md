# parse_ooaffentry

## Location
[src/backend/tsearch/spell.c:858-913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L858-L913)

## Overview
Parses entries from MySpell or Hunspell format .affix files, extracting type, flag, find, replace, and mask fields from each line.

## Definition
```c
static int parse_ooaffentry(char *str, char *type, char *flag, char *find, char *repl, char *mask)
```

## Detailed Description
parse_ooaffentry implements a state machine parser for affix file entries in OpenOffice/Hunspell format. The function processes two types of lines:

1. **Header lines**: `<type> <flag> <cross_flag> <flag_count>`
2. **Field lines**: `<type> <flag> <find> <replace> <mask>`

The parser uses a five-state machine that progresses through each expected field:
- PAE_WAIT_TYPE: Expecting the affix type (PFX/SFX)
- PAE_WAIT_FLAG: Expecting the affix flag identifier
- PAE_WAIT_FIND: Expecting the string to find/strip
- PAE_WAIT_REPL: Expecting the replacement string
- PAE_WAIT_MASK: Expecting the condition mask pattern

The function handles incomplete lines gracefully, setting unfound fields to empty strings and returning the actual number of fields successfully parsed.

## Parameters / Member Variables
- `str`: Input line to parse from the .affix file
- `type`: Output buffer for affix type (PFX or SFX) - must be BUFSIZ
- `flag`: Output buffer for affix flag identifier - must be BUFSIZ
- `find`: Output buffer for characters to find/strip - must be BUFSIZ
- `repl`: Output buffer for replacement string - must be BUFSIZ
- `mask`: Output buffer for condition pattern - must be BUFSIZ

## Dependencies
- Functions called/Symbols referenced:
  - [get_nextfield](../g/get_nextfield.md)
  - PAE_WAIT_TYPE
  - PAE_WAIT_FLAG
  - PAE_WAIT_FIND
  - PAE_WAIT_REPL
  - PAE_WAIT_MASK
  - elog
- Called from (representative examples):
  - [NIImportOOAffixes](../N/NIImportOOAffixes.md)

## Notes and Other Information
- Returns the number of fields successfully parsed (0-5)
- Static function, only accessible within the spell.c module
- All output buffers are initialized to empty strings before parsing
- Uses state machine approach for robust field extraction
- Handles early end-of-line conditions by stopping at the first failed field
- Compatible with both MySpell and Hunspell affix file formats
- Error handling includes logging for unexpected parser states
- Relies on get_nextfield for actual field extraction and whitespace handling