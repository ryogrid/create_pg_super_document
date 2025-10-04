# plperlu_inline_handler

## Location
[src/pl/plperl/plperl.c:2075-2082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2075-L2082)

## Overview
Entry point function for executing PL/PerlU (untrusted Perl) inline code blocks, delegating to the main Perl inline handler.

## Definition
```c
Datum plperlu_inline_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the specific entry point for executing PL/PerlU (untrusted Perl) inline code blocks via the DO statement. It is a thin wrapper that immediately delegates to the main `plperl_inline_handler` function, which handles the actual execution logic for both trusted (PL/Perl) and untrusted (PL/PerlU) Perl inline code. The distinction between trusted and untrusted execution is handled within the main inline handler based on the language properties, allowing this function to simply pass through to the common implementation.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the InlineCodeBlock pointer and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [plperl_inline_handler](plperl_inline_handler.md)
  - PG_FUNCTION_INFO_V1
- Called from (representative examples):
  - [plperlu_call_handler](plperlu_call_handler.md)

## Notes and Other Information
- This is the handler function specifically registered for PL/PerlU (untrusted Perl) inline code execution
- Used when executing DO blocks with PL/PerlU language specification
- The actual differentiation between trusted and untrusted execution happens within the main inline handler
- Located in src/pl/plperl/plperl.c:2075-2082
- Forms part of PostgreSQL's procedural language interface for executing anonymous Perl code blocks

## Simplified Source

```c
Datum plperlu_inline_handler(PG_FUNCTION_ARGS)
{
    return plperl_inline_handler(fcinfo);
}
```