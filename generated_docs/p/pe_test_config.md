# pe_test_config

## Location
[src/test/modules/test_escape/test_escape.c:24-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L24-L33)

## Overview
A configuration structure that manages test execution parameters and state for the PostgreSQL escape function test module.

## Definition

```c
typedef struct pe_test_config
{
	int			verbosity;
	bool		force_unsupported;
	const char *conninfo;
	PGconn	   *conn;

	int			test_count;
	int			failure_count;
} pe_test_config;
```
## Detailed Description
The  structure serves as the central configuration and state management object for the test_escape module. It encapsulates both test execution parameters (verbosity level, connection information) and runtime state (test counts, failure tracking). This structure is passed to various test functions to maintain consistent configuration and accumulate test results across different test scenarios.

## Parameters / Member Variables
- : Controls the level of output detail during test execution
- : Boolean flag to force execution of tests that might not be supported in current environment
- : Connection string used to establish database connection for tests
- : Active PostgreSQL connection handle used throughout test execution
- : Running count of total tests executed
- : Running count of tests that have failed

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this structure)
- Called from (representative examples):
  - [report_result](../r/report_result.md)
  - [test_gb18030_page_multiple](../t/test_gb18030_page_multiple.md)
  - [test_gb18030_json](../t/test_gb18030_json.md)
  - [test_psql_parse](../t/test_psql_parse.md)
  - [test_one_vector_escape](../t/test_one_vector_escape.md)
  - [test_one_vector](../t/test_one_vector.md)
  - [main](../m/main.md)

## Notes and Other Information
This structure is fundamental to the test_escape module's architecture, providing a consistent interface for test configuration and result tracking. It enables the module to maintain state across multiple test functions while providing flexible configuration options for different testing scenarios.