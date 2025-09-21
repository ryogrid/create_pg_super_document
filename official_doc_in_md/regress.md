Chapter 31. Regression Tests  
---  
[Prev](jit-extensibility.md "30.4. Extensibility") | [Up](admin.md "Part III. Server Administration")| Part III. Server Administration| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](regress-run.md "31.1. Running the Tests")  
  
* * *

## Chapter 31. Regression Tests

**Table of Contents**

[31.1. Running the Tests](regress-run.md)
    

[31.1.1. Running the Tests Against a Temporary Installation](regress-run.md#REGRESS-RUN-TEMP-INST)
[31.1.2. Running the Tests Against an Existing Installation](regress-run.md#REGRESS-RUN-EXISTING-INST)
[31.1.3. Additional Test Suites](regress-run.md#REGRESS-ADDITIONAL)
[31.1.4. Locale and Encoding](regress-run.md#REGRESS-RUN-LOCALE)
[31.1.5. Custom Server Settings](regress-run.md#REGRESS-RUN-CUSTOM-SETTINGS)
[31.1.6. Extra Tests](regress-run.md#REGRESS-RUN-EXTRA-TESTS)
[31.2. Test Evaluation](regress-evaluation.md)
    

[31.2.1. Error Message Differences](regress-evaluation.md#REGRESS-EVALUATION-MESSAGE-DIFFERENCES)
[31.2.2. Locale Differences](regress-evaluation.md#REGRESS-EVALUATION-LOCALE-DIFFERENCES)
[31.2.3. Date and Time Differences](regress-evaluation.md#REGRESS-EVALUATION-DATE-TIME-DIFFERENCES)
[31.2.4. Floating-Point Differences](regress-evaluation.md#REGRESS-EVALUATION-FLOAT-DIFFERENCES)
[31.2.5. Row Ordering Differences](regress-evaluation.md#REGRESS-EVALUATION-ORDERING-DIFFERENCES)
[31.2.6. Insufficient Stack Depth](regress-evaluation.md#REGRESS-EVALUATION-STACK-DEPTH)
[31.2.7. The “random” Test](regress-evaluation.md#REGRESS-EVALUATION-RANDOM-TEST)
[31.2.8. Configuration Parameters](regress-evaluation.md#REGRESS-EVALUATION-CONFIG-PARAMS)
[31.3. Variant Comparison Files](regress-variant.md)
[31.4. TAP Tests](regress-tap.md)
    

[31.4.1. Environment Variables](regress-tap.md#REGRESS-TAP-VARS)
[31.5. Test Coverage Examination](regress-coverage.md)
    

[31.5.1. Coverage with Autoconf and Make](regress-coverage.md#REGRESS-COVERAGE-CONFIGURE)
[31.5.2. Coverage with Meson](regress-coverage.md#REGRESS-COVERAGE-MESON)

The regression tests are a comprehensive set of tests for the SQL implementation in PostgreSQL. They test standard SQL operations as well as the extended capabilities of PostgreSQL. 

* * *

[Prev](jit-extensibility.md "30.4. Extensibility") | [Up](admin.md "Part III. Server Administration")|  [Next](regress-run.md "31.1. Running the Tests")  
---|---|---  
30.4. Extensibility | [Home](index.md "PostgreSQL 17.5 Documentation")|  31.1. Running the Tests
