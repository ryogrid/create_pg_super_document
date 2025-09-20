# SPIPrepareOptions

## Location
[src/include/executor/spi.h:37-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/spi.h#L37-L43)

## Overview
SPIPrepareOptions is a structure that provides optional configuration parameters for the SPI_prepare_extended function, allowing customization of SQL parsing and cursor behavior during statement preparation.

## Definition

```c
typedef struct SPIPrepareOptions
{
	ParserSetupHook parserSetup;
	void	   *parserSetupArg;
	RawParseMode parseMode;
	int			cursorOptions;
} SPIPrepareOptions;
```
## Detailed Description
SPIPrepareOptions serves as a configuration structure for advanced SPI statement preparation scenarios. It enables callers to customize the parsing process by providing custom parser setup hooks, specifying parsing modes, and configuring cursor-specific options. This structure is particularly useful when preparing statements that require non-standard parsing behavior or when working with cursor-based operations that need specific configuration.

The structure follows PostgreSQL's pattern of using optional parameter structures to extend function interfaces without breaking backward compatibility. When passed to SPI_prepare_extended, these options override default parsing and preparation behavior.

## Parameters / Member Variables
- `parserSetup`: Function pointer to a custom parser setup hook that will be called during SQL parsing to configure parser state
- `*parserSetupArg`: Opaque pointer argument passed to the parserSetup hook function, allowing context-specific data to be provided
- `parseMode`: Enumeration value specifying the raw parsing mode to use during statement preparation (e.g., normal, plpgsql, etc.)
- `cursorOptions`: Integer bitmask of cursor-specific options that affect how prepared statements will behave when executed as cursors
## Dependencies
- Functions called/Symbols referenced:
  - ParserSetupHook
  - RawParseMode

- Called from (representative examples):
  - [SPI_prepare_extended](SPI_prepare_extended.md)

## Notes and Other Information
- This structure is designed for advanced SPI usage and is not needed for basic statement preparation
- The parserSetup hook mechanism allows for sophisticated customization of the parsing process
- The parseMode parameter is particularly important for procedural languages that need specific parsing behavior
- cursorOptions allows fine-grained control over cursor behavior for prepared statements
- All members are optional and can be set to appropriate default values (NULL/0) when not needed
- The structure provides a clean extension point for future SPI preparation options without API changes