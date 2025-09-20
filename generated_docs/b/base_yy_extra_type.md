# base_yy_extra_type

## Location
[src/backend/parser/gramparse.h:35-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/gramparse.h#L35-L56)

## Overview
A structure that extends the core scanner's YY_EXTRA data to support one-token lookahead functionality and grammar state management in PostgreSQL's parser.

## Definition

```c
typedef struct base_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables for base_yylex().
	 */
	bool		have_lookahead; /* is lookahead info valid? */
	int			lookahead_token;	/* one-token lookahead */
	core_YYSTYPE lookahead_yylval;	/* yylval for lookahead token */
	YYLTYPE		lookahead_yylloc;	/* yylloc for lookahead token */
	char	   *lookahead_end;	/* end of current token */
	char		lookahead_hold_char;	/* to be put back at *lookahead_end */

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
} base_yy_extra_type;
```
## Detailed Description
The  structure serves as an extended version of the flex scanner's YY_EXTRA data, specifically designed for PostgreSQL's base parser functionality. It builds upon the  by adding lookahead capabilities and grammar-specific state management. This structure enables the parser to implement one-token lookahead, which is essential for resolving certain parsing ambiguities in SQL syntax. The structure maintains both the core scanning functionality and additional state needed for advanced parsing operations, including storing the final parse tree result.

## Parameters / Member Variables
- : Embedded core scanner extra data containing fundamental scanning state
- : Boolean flag indicating whether lookahead token information is currently valid
- : Stores the token code for the one-token lookahead
- : Contains the semantic value (yylval) associated with the lookahead token
- : Location information (yylloc) for the lookahead token
- : Pointer to the end position of the current token in the input buffer
- : Character that needs to be restored at the lookahead_end position
- : List pointer where the final parsing result (parse tree) is stored

## Dependencies
- Functions called/Symbols referenced:
  - [core_yy_extra_type](../c/core_yy_extra_type.md)
  - core_YYSTYPE
  - YYLTYPE
- Called from (representative examples):
  - pg_yyget_extra
  - [raw_parser](../r/raw_parser.md)
  - [base_yylex](base_yylex.md)

## Notes and Other Information
This structure is specifically designed for the base parser layer and implements a sophisticated lookahead mechanism that allows the parser to peek at the next token without consuming it. This capability is crucial for handling SQL syntax ambiguities where the parser needs to make decisions based on upcoming tokens. The structure follows PostgreSQL's layered parser architecture, where the base parser builds upon the core scanner functionality while adding its own specialized features. The parsetree member serves as the final output container for the completed parse operation, making this structure a central component in PostgreSQL's SQL parsing pipeline.