# AppendIntegerCommandOption

## Location
[src/bin/pg_basebackup/streamutil.c:856-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L856-L868)

## Overview
A utility function that appends a command option with an associated integer value to a PostgreSQL server command buffer.

## Definition
```c
void AppendIntegerCommandOption(PQExpBuffer buf, bool use_new_option_syntax, char *option_name, int32 option_value)
```

## Detailed Description
This function provides a convenient way to append command options that take integer values to PostgreSQL server commands. It first calls AppendPlainCommandOption to append the option name using the appropriate syntax, then directly appends the integer value using appendPQExpBuffer with a "%d" format specifier. Unlike string values, integer values don't require escaping or quoting, making this implementation simpler than AppendStringCommandOption.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the option to
- `use_new_option_syntax`: Boolean flag indicating whether to use new or legacy option syntax
- `option_name`: Name of the command option to append
- `option_value`: Integer value for the option (int32)

## Dependencies
- Functions called/Symbols referenced:
  - [AppendPlainCommandOption](AppendPlainCommandOption.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (multiple calls in pg_basebackup.c)

## Notes and Other Information
- [Integer](../I/Integer.md) values are appended directly without any escaping or quoting since they cannot contain special characters
- Uses int32 type for consistency with PostgreSQL's integer handling
- Part of the pg_basebackup utility's command construction infrastructure alongside AppendStringCommandOption and AppendPlainCommandOption

## Simplified Source

```c
void AppendIntegerCommandOption(PQExpBuffer buf, bool use_new_option_syntax,
                               char *option_name, int32 option_value) {
    // First append the option name using existing function
    AppendPlainCommandOption(buf, use_new_option_syntax, option_name);

    // Append the integer value directly (no escaping needed)
    appendPQExpBuffer(buf, " %d", option_value);
}
```