# filtered_base_yylex

## Location
src/interfaces/ecpg/preproc/parser.c: 56 - 227

## Overview
An intermediate lexical filter between the parser and base lexer that handles multi-token lookahead scenarios and converts Unicode identifiers/string constants to their standard forms.

## Definition
```c
int filtered_base_yylex(void)
```

## Detailed Description
This function serves as a crucial component in the ECPG preprocessor lexical analysis pipeline. It acts as an intermediate filter between the parser and the base lexer (base_yylex in scan.l) to address two main challenges:

1. **Multi-token lookahead reduction**: The standard SQL grammar sometimes requires more than one token lookahead, which would violate LALR(1) grammar requirements. This filter reduces these cases to one-token lookahead by replacing tokens based on context.

2. **Unicode token conversion**: Converts UIDENT and USCONST sequences (potentially with UESCAPE clauses) into plain IDENT and SCONST tokens for simpler grammar processing.

The function implements a lookahead mechanism using global variables to peek at the next token and make contextual decisions about token replacement. This approach is more efficient than trying to recognize multiword tokens directly in the scanner, as it avoids the complexity of handling comments between words and prevents the need for scanner backtracking.

## Parameters / Member Variables
This function takes no parameters and returns an integer token code.

## Dependencies
- Functions called/Symbols referenced:
  - `base_yylex`: The underlying lexer function that generates raw tokens
  - `check_uescapechar`: Validates Unicode escape characters
  - `mmerror`: Reports parsing errors
  - `psprintf`: PostgreSQL string formatting function
  - `strlen`: Standard C string length function

- Called from (representative examples):
  - Parser functions in the ECPG preprocessor (indirectly through lexer interface)

## Notes and Other Information
- The function maintains lookahead state using global variables: `have_lookahead`, `lookahead_token`, `lookahead_yylval`, `lookahead_yylloc`, and `lookahead_yytext`
- Token replacement patterns include:
  - `FORMAT` → `FORMAT_LA` when followed by `JSON`
  - `NOT` → `NOT_LA` when followed by `BETWEEN`, `IN_P`, `LIKE`, `ILIKE`, or `SIMILAR`
  - `NULLS_P` → `NULLS_LA` when followed by `FIRST_P` or `LAST_P`
  - `WITH` → `WITH_LA` when followed by `TIME` or `ORDINALITY`
  - `WITHOUT` → `WITHOUT_LA` when followed by `TIME`
  - `UIDENT` → `IDENT` (after processing potential UESCAPE)
  - `USCONST` → `SCONST` (after processing potential UESCAPE)
- Unicode escape processing validates that the escape character is exactly 3 characters long (including quotes) and uses `check_uescapechar` for validation
- The filter is essential for maintaining ECPG preprocessors ability to handle complex SQL syntax while keeping the grammar manageable