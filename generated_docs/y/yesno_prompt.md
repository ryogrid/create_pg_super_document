# yesno_prompt

## Location
[src/bin/scripts/common.c:137-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/common.c#L137-L168)

## Overview
Displays a yes/no question to the user and returns a boolean result based on their response, repeatedly prompting until a valid answer is given.

## Definition

```c
bool
yesno_prompt(const char *question)
```
## Detailed Description
This function presents an interactive yes/no prompt to the user, displaying the provided question along with localized options for "yes" and "no". It uses simple_prompt to read user input and compares the response against the translated versions of PG_YESLETTER and PG_NOLETTER constants. 

The function operates in a loop, continuously prompting the user until they provide a valid response that matches either the localized "yes" or "no" option. If the user enters an invalid response, the function displays an error message asking them to answer with one of the valid options and prompts again.

The prompt format includes the question followed by the localized yes/no options in parentheses, formatted as "question (y/n) " where y/n are replaced with the appropriate localized single letters.

## Parameters / Member Variables
- : The question text to display to the user (should be translatable)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - simple_prompt
  - strcmp
  - free
  - printf
  - PG_YESLETTER
  - PG_NOLETTER
  - _ (gettext macro for translation)
- Called from (representative examples):
  - [main](../m/main.md) (in createuser.c)
  - [main](../m/main.md) (in dropdb.c) 
  - [main](../m/main.md) (in dropuser.c)

## Notes and Other Information
- The function supports internationalization through gettext translation macros
- Uses PG_YESLETTER and PG_NOLETTER constants which are locale-specific single characters
- Automatically handles memory management by freeing the response string after each prompt
- Will loop indefinitely until the user provides a valid yes or no response
- The question parameter should be marked for translation if used with literal strings
- Commonly used in PostgreSQL utility programs for confirmation prompts before potentially destructive operations
- Returns true for "yes" responses and false for "no" responses