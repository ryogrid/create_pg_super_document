# trailing_pad

## Location
[src/port/snprintf.c:1528-1532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1528-L1532)

## Overview
Outputs trailing space padding for left-justified formatted values by detecting negative padding lengths and converting them to space characters.

## Definition
```c
static void trailing_pad(int padlen, PrintfTarget *target)
```

## Detailed Description
The `trailing_pad` function is a simple utility function in PostgreSQL's custom sprintf implementation that handles the output of trailing padding spaces for left-justified formatted values. When the `compute_padlen` function returns a negative padding value (indicating left-justification), this function converts that negative value to the appropriate number of trailing spaces.

This function works in conjunction with the `compute_padlen` function's convention of using negative values to represent left-justification. It provides a clean separation of concerns by handling only the trailing padding logic, making the overall formatting code more modular and maintainable.

## Parameters / Member Variables
- `padlen`: The padding length value; when negative, indicates the number of trailing spaces needed for left-justification
- `target`: Pointer to the PrintfTarget structure that handles the actual character output

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (structure used for output handling)
  - [dopr_outchmulti](../d/dopr_outchmulti.md) (outputs multiple copies of a character - in this case, spaces)
- Called from (representative examples):
  - [flushbuffer](../f/flushbuffer.md) (at src/port/snprintf.c:338)
  - [fmtstr](../f/fmtstr.md) (at src/port/snprintf.c:989)
  - [fmtint](../f/fmtint.md) (at src/port/snprintf.c:1114)
  - [fmtchar](../f/fmtchar.md) (at src/port/snprintf.c:1132)
  - [fmtfloat](../f/fmtfloat.md) (at src/port/snprintf.c:1265)

## Notes and Other Information
- This is a static function within the snprintf.c module, indicating it's an internal utility
- Only outputs padding when padlen is negative, implementing the left-justification convention
- Works with the negative padding convention established by `compute_padlen` function
- Always uses space characters for trailing padding (never zero-padding for trailing)
- Part of PostgreSQL's portable snprintf implementation ensuring consistent formatting across platforms
- Extremely simple function that serves as the counterpart to the more complex `leading_pad` function
- Essential for implementing format specifiers like `%-10s` which left-justify content within a field width