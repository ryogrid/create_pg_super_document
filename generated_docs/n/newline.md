# newline

## Location
[src/backend/regex/regc_lex.c:1010-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_lex.c#L1010-L1021)

## Overview
The `newline` function is a simple utility that returns the character representation of a newline, serving as an abstraction layer for the CHR macro usage.

## Definition
```c
static chr newline(void)
```

## Detailed Description
The `newline` function is a minimal wrapper function that encapsulates the creation of a newline character using the CHR macro. Its primary purpose is to confine the direct usage of the CHR macro to the regex lexer source file, providing a clean interface for other parts of the code that need newline characters. This design promotes better code organization and maintainability by centralizing character constant creation.

The function simply returns the result of `CHR('\n')`, which converts the literal newline character into the appropriate character type used by the regex engine.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CHR (macro for creating character constants)
  - [chr](../c/chr.md) (character type used by regex engine)
- Called from (representative examples):
  - [load_hba](../l/load_hba.md) (HBA configuration loading)
  - [load_ident](../l/load_ident.md) (identity mapping loading)
  - CNOERR (regex compilation error handling)
  - [xmltotext_with_options](../x/xmltotext_with_options.md) (XML processing)
  - [replace_token](../r/replace_token.md) (initdb token replacement)
  - [replace_guc_value](../r/replace_guc_value.md) (GUC configuration processing)
  - Various formatting and parsing utilities throughout PostgreSQL

## Notes and Other Information
- Part of PostgreSQL's regex engine implementation in src/backend/regex/regc_lex.c:1010-1021
- Serves as an abstraction layer to limit direct CHR macro usage
- Widely used across PostgreSQL for consistent newline handling in configuration files, XML processing, and code formatting tools
- Simple design reflects PostgreSQL's approach to encapsulating implementation details behind clean interfaces
- The extensive usage throughout the codebase indicates its role as a fundamental utility for text processing operations