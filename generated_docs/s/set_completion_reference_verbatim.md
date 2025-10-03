# set_completion_reference_verbatim

## Location
[src/bin/psql/tab-complete.c:5612-5624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5612-L5624)

## Overview
Sets up the completion reference object to be exactly the given word verbatim, used in PostgreSQL's psql tab completion system.

## Definition

```c
static void
set_completion_reference_verbatim(const char *word)
```
## Detailed Description
This function is a utility in PostgreSQL's psql tab completion system that configures the global completion reference variables when the reference object should be exactly the provided word without any schema qualification or modification. It clears any schema reference and sets the object reference to a duplicate of the input word.

## Parameters / Member Variables
- `*word`: The word to be set as the completion reference object verbatim
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for duplicating the word string)
- Global variables modified:
  - completion_ref_schema (set to NULL)
  - completion_ref_object (set to duplicated word)
- Called from (representative examples):
  - THING_NO_SHOW macro usage
  - HeadMatchesCS function usage

## Notes and Other Information
- This is a static function used internally within the tab completion system
- Part of the reference completion mechanism that helps determine what objects should be suggested during tab completion
- The function ensures that schema references are cleared when only object-level completion is needed
- Used in cases where the completion reference should be taken literally without parsing for schema qualifications