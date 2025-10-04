# test_one_vector

## Location
[src/test/modules/test_escape/test_escape.c:866-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L866-L883)

## Overview
A test orchestration function that sets up the database client encoding for a test vector and runs that vector against all available escape functions.

## Definition

```c
static void
test_one_vector(pe_test_config *tc, const pe_test_vector *tv)
```
## Detailed Description
This function serves as a test coordinator that prepares the database connection for a specific test vector by setting the appropriate client encoding, then systematically tests the vector against all available escape functions. It first sets the client encoding on the database connection to match the encoding specified in the test vector, ensuring that the escape function tests are performed under the correct encoding context. If encoding setup fails, the function terminates the entire test program. After successful encoding setup, it iterates through all registered escape functions and invokes the comprehensive testing for each one.

## Parameters / Member Variables
- `*tc`: Test configuration structure containing the database connection and test settings
- `*tv`: Test vector containing the input data, expected encoding, and test parameters
## Dependencies
- Functions called/Symbols referenced:
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - fprintf
  - exit
  - lengthof
  - [test_one_vector_escape](test_one_vector_escape.md)
- Types referenced:
  - [pe_test_config](../p/pe_test_config.md)
  - [pe_test_vector](../p/pe_test_vector.md)
  - [pe_test_escape_func](../p/pe_test_escape_func.md)
- Constants referenced:
  - pe_test_escape_funcs (array of escape functions to test)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a test module function located in 
- Critical for ensuring consistent encoding context across all escape function tests
- Terminates the entire test program if client encoding cannot be set (fail-fast approach)
- Iterates through the global array  to test all registered escape functions
- Acts as the primary test orchestrator between the main test loop and individual escape function testing
- Uses  macro to determine the number of escape functions to test
- Error handling includes detailed error messages showing both the failed encoding and PostgreSQL error details
- Essential for systematic testing of escape functions across different character encodings

## Simplified Source
```c
static void
test_one_vector(pe_test_config *tc, const pe_test_vector *tv)
{
    /* Set the client encoding for this test vector */
    if (PQsetClientEncoding(tc->conn, tv->client_encoding))
    {
        fprintf(stderr, "failed to set encoding to %s:\n%s\n",
                tv->client_encoding, PQerrorMessage(tc->conn));
        exit(1);
    }

    /* Test this vector against all escape functions */
    for (int escoff = 0; escoff < lengthof(pe_test_escape_funcs); escoff++)
    {
        const pe_test_escape_func *ef = &pe_test_escape_funcs[escoff];
        test_one_vector_escape(tc, tv, ef);
    }
}
```