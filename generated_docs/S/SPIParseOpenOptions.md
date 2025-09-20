# SPIParseOpenOptions

## Location
[src/include/executor/spi.h:58-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/spi.h#L58-L63)

## Overview
SPIParseOpenOptions is a structure that provides optional configuration parameters for the SPI_cursor_parse_open function, controlling cursor parsing, parameter binding, and access permissions.

## Definition

```c
typedef struct SPIParseOpenOptions
{
	ParamListInfo params;
	int			cursorOptions;
	bool		read_only;
} SPIParseOpenOptions;
```
## Detailed Description
SPIParseOpenOptions serves as a configuration structure specifically designed for cursor-based SPI operations that combine SQL parsing and cursor opening in a single operation. It enables callers to specify parameter values, configure cursor-specific behavior, and enforce read-only access restrictions during the parse-and-open process. This structure is particularly useful for procedural languages and applications that need to create cursors with specific characteristics directly from SQL text.

The structure follows PostgreSQL's pattern of using dedicated option structures for complex operations, providing a clean interface that can be extended without breaking backward compatibility.

## Parameters / Member Variables
- `params`: ParamListInfo structure containing parameter values to bind to the SQL statement during parsing and cursor creation
- `cursorOptions`: Integer bitmask specifying cursor-specific options that control cursor behavior (scrollability, holdability, etc.)
- `read_only`: Boolean flag enforcing read-only access, preventing the cursor from executing modification statements
## Dependencies
- Functions called/Symbols referenced:
  - [ParamListInfo](../P/ParamListInfo.md)

- Called from (representative examples):
  - [SPI_cursor_parse_open](SPI_cursor_parse_open.md)

## Notes and Other Information
- This structure is specifically designed for cursor operations that combine parsing and opening in one step
- The cursorOptions parameter allows fine-grained control over cursor characteristics such as scrollability and transaction behavior
- The read_only flag provides an additional safety mechanism for applications that want to ensure cursors cannot modify data
- Parameter binding occurs during the parse phase, allowing for dynamic SQL execution with bound parameters
- This structure enables efficient cursor creation without requiring separate prepare and open steps
- All members are optional and can be set to appropriate defaults when specific behavior is not required
- The design supports future extension of cursor-specific options without API changes