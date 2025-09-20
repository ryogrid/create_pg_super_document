# NUMProc

## Location
[src/backend/utils/adt/formatting.c:996-1023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L996-L1023)

## Overview
A comprehensive processor structure used by PostgreSQL's numeric formatting system to manage the state and context during number-to-character and character-to-number conversion operations.

## Definition

```c
typedef struct NUMProc
{
	bool		is_to_char;
	NUMDesc    *Num;			/* number description		*/

	int			sign,			/* '-' or '+'			*/
				sign_wrote,		/* was sign write		*/
				num_count,		/* number of write digits	*/
				num_in,			/* is inside number		*/
				num_curr,		/* current position in number	*/
				out_pre_spaces, /* spaces before first digit	*/

				read_dec,		/* to_number - was read dec. point	*/
				read_post,		/* to_number - number of dec. digit */
				read_pre;		/* to_number - number non-dec. digit */

	char	   *number,			/* string with number	*/
			   *number_p,		/* pointer to current number position */
			   *inout,			/* in / out buffer	*/
			   *inout_p,		/* pointer to current inout position */
			   *last_relevant,	/* last relevant number after decimal point */

			   *L_negative_sign,	/* Locale */
			   *L_positive_sign,
			   *decimal,
			   *L_thousands_sep,
			   *L_currency_symbol;
} NUMProc;
```
## Detailed Description
The NUMProc structure is a sophisticated state machine used by PostgreSQL's numeric formatting system for bidirectional conversion between numbers and formatted strings. It maintains all the necessary context information during formatting operations, including parsing state, buffer management, locale-specific formatting characters, and position tracking. The structure supports both to_char (number-to-string) and to_number (string-to-number) operations, adapting its behavior based on the is_to_char flag. It handles complex formatting scenarios including locale-specific number formatting, decimal precision control, and currency symbol placement.

## Parameters / Member Variables
- `is_to_char`: Boolean flag indicating the direction of conversion (true for number-to-char, false for char-to-number)
- `Num`: Pointer to NUMDesc structure containing the number format description
- `sign`: Current sign character ('-' or '+')
- `sign_wrote`: Flag indicating whether the sign has been written to output
- `num_count`: Count of digits written to output
- `num_in`: Flag indicating if currently inside the number portion
- `num_curr`: Current position within the number
- `out_pre_spaces`: Number of spaces to output before the first digit
- `read_dec`: Flag for to_number operations indicating if decimal point was read
- `read_post`: For to_number operations, count of digits after decimal point
- `read_pre`: For to_number operations, count of digits before decimal point
- `number`: String buffer containing the number
- `number_p`: Pointer to current position in the number string
- `inout`: Input/output buffer for conversion operations
- `inout_p`: Pointer to current position in the input/output buffer
- `last_relevant`: Pointer to last significant digit after decimal point
- `L_negative_sign`: Locale-specific negative sign string
- `L_positive_sign`: Locale-specific positive sign string
- `decimal`: Locale-specific decimal point character
- `L_thousands_sep`: Locale-specific thousands separator
- `L_currency_symbol`: Locale-specific currency symbol

## Dependencies
- Functions called/Symbols referenced:
  - NUMDesc (number format description structure)
  - decimal (locale decimal point)
- Called from (representative examples):
  - DCH_ZONED
  - [NUM_prepare_locale](NUM_prepare_locale.md)
  - [NUM_numpart_from_char](NUM_numpart_from_char.md)
  - [NUM_numpart_to_char](NUM_numpart_to_char.md)
  - [NUM_eat_non_data_chars](NUM_eat_non_data_chars.md)
  - [NUM_processor](NUM_processor.md)

## Notes and Other Information
This structure is central to PostgreSQL's numeric formatting functionality in src/backend/utils/adt/formatting.c. It serves as a comprehensive state machine that manages the complex process of converting between numeric values and their formatted string representations. The structure handles locale-specific formatting requirements, maintains parsing state for bidirectional conversions, and provides buffer management for efficient string operations. The numerous pointer fields enable precise tracking of positions during parsing and formatting operations, while the locale-specific fields ensure proper internationalization support.