# scanner_isspace

## Location
[src/backend/parser/scansup.c:117-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/scansup.c#L117-L128)

## Overview
A utility function that determines whether a character is considered whitespace according to PostgreSQL's lexical scanner, ensuring consistent whitespace handling across the system.

## Definition

```c
bool
scanner_isspace(char ch)
```
## Detailed Description
This function provides a precise definition of whitespace characters that matches PostgreSQL's flex-based lexical scanner (scan.l). Unlike the standard library's  function, which can be locale-dependent and may include additional characters,  provides a fixed, predictable set of whitespace characters that exactly matches what the SQL parser considers as whitespace.

The function explicitly checks for six specific whitespace characters: space (' '), tab ('\t'), newline ('\n'), carriage return ('\r'), vertical tab ('\v'), and form feed ('\f'). This ensures consistent behavior regardless of the system locale and guarantees that string parsing functions behave identically to the main SQL lexer.

## Parameters / Member Variables
- : The character to test for whitespace classification

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only character literal comparisons)
- Called from (representative examples):
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md) (schema creation parsing)
  - [CleanQuerytext](../C/CleanQuerytext.md) (query text normalization)
  - [array_in](../a/array_in.md)/array_out (array parsing and formatting)
  - [parse_ident](../p/parse_ident.md) (identifier parsing utilities)
  - [SplitIdentifierString](../S/SplitIdentifierString.md) (identifier splitting functions)
  - [SplitDirectoriesString](../S/SplitDirectoriesString.md)/SplitGUCList (configuration parsing)

## Notes and Other Information
- Designed to match exactly the whitespace definition in scan.l (the flex scanner specification)
- Provides locale-independent whitespace detection, crucial for consistent SQL parsing
- Used extensively throughout PostgreSQL for parsing various string formats including arrays, identifiers, and configuration values
- The comment suggests that similar functions for other character classes (like isalnum) might be needed in the future
- Essential for maintaining consistency between the main SQL lexer and various string parsing utilities throughout the codebase
- Returns a simple boolean value with no side effects or error conditions