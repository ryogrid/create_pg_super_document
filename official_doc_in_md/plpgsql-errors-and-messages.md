41.9. Errors and Messages  
---  
[Prev](plpgsql-transactions.md "41.8. Transaction Management") | [Up](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")| Chapter 41. PL/pgSQL — SQL Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpgsql-trigger.md "41.10. Trigger Functions")  
  
* * *

## 41.9. Errors and Messages #

[41.9.1. Reporting Errors and Messages](plpgsql-errors-and-messages.md#PLPGSQL-STATEMENTS-RAISE)
[41.9.2. Checking Assertions](plpgsql-errors-and-messages.md#PLPGSQL-STATEMENTS-ASSERT)

### 41.9.1. Reporting Errors and Messages #

Use the `RAISE` statement to report messages and raise errors. 
    
    
    RAISE [ _level_ ] '_format_ ' [, _expression_ [, ... ]] [ USING _option_ = _expression_ [, ... ] ];
    RAISE [ _level_ ] _condition_name_ [ USING _option_ = _expression_ [, ... ] ];
    RAISE [ _level_ ] SQLSTATE '_sqlstate_ ' [ USING _option_ = _expression_ [, ... ] ];
    RAISE [ _level_ ] USING _option_ = _expression_ [, ... ];
    RAISE ;
    

The _`level`_ option specifies the error severity. Allowed levels are `DEBUG`, `LOG`, `INFO`, `NOTICE`, `WARNING`, and `EXCEPTION`, with `EXCEPTION` being the default. `EXCEPTION` raises an error (which normally aborts the current transaction); the other levels only generate messages of different priority levels. Whether messages of a particular priority are reported to the client, written to the server log, or both is controlled by the [log_min_messages](runtime-config-logging.md#GUC-LOG-MIN-MESSAGES) and [client_min_messages](runtime-config-client.md#GUC-CLIENT-MIN-MESSAGES) configuration variables. See [Chapter 19](runtime-config.md "Chapter 19. Server Configuration") for more information. 

After _`level`_ if any, you can specify a _`format`_ string (which must be a simple string literal, not an expression). The format string specifies the error message text to be reported. The format string can be followed by optional argument expressions to be inserted into the message. Inside the format string, `%` is replaced by the string representation of the next optional argument's value. Write `%%` to emit a literal `%`. The number of arguments must match the number of `%` placeholders in the format string, or an error is raised during the compilation of the function. 

In this example, the value of `v_job_id` will replace the `%` in the string: 
    
    
    RAISE NOTICE 'Calling cs_create_job(%)', v_job_id;
    

You can attach additional information to the error report by writing `USING` followed by _`option`_ = _`expression`_ items. Each _`expression`_ can be any string-valued expression. The allowed _`option`_ key words are: 

`MESSAGE` #
    

Sets the error message text. This option can't be used in the form of `RAISE` that includes a format string before `USING`.

`DETAIL` #
    

Supplies an error detail message.

`HINT` #
    

Supplies a hint message.

`ERRCODE` #
    

Specifies the error code (SQLSTATE) to report, either by condition name, as shown in [Appendix A](errcodes-appendix.md "Appendix A. PostgreSQL Error Codes"), or directly as a five-character SQLSTATE code.

`COLUMN`  
`CONSTRAINT`  
`DATATYPE`  
`TABLE`  
`SCHEMA` #
    

Supplies the name of a related object.

This example will abort the transaction with the given error message and hint: 
    
    
    RAISE EXCEPTION 'Nonexistent ID --> %', user_id
          USING HINT = 'Please check your user ID';
    

These two examples show equivalent ways of setting the SQLSTATE: 
    
    
    RAISE 'Duplicate user ID: %', user_id USING ERRCODE = 'unique_violation';
    RAISE 'Duplicate user ID: %', user_id USING ERRCODE = '23505';
    

There is a second `RAISE` syntax in which the main argument is the condition name or SQLSTATE to be reported, for example: 
    
    
    RAISE division_by_zero;
    RAISE SQLSTATE '22012';
    

In this syntax, `USING` can be used to supply a custom error message, detail, or hint. Another way to do the earlier example is 
    
    
    RAISE unique_violation USING MESSAGE = 'Duplicate user ID: ' || user_id;
    

Still another variant is to write `RAISE USING` or `RAISE _`level`_ USING` and put everything else into the `USING` list. 

The last variant of `RAISE` has no parameters at all. This form can only be used inside a `BEGIN` block's `EXCEPTION` clause; it causes the error currently being handled to be re-thrown. 

### Note

Before PostgreSQL 9.1, `RAISE` without parameters was interpreted as re-throwing the error from the block containing the active exception handler. Thus an `EXCEPTION` clause nested within that handler could not catch it, even if the `RAISE` was within the nested `EXCEPTION` clause's block. This was deemed surprising as well as being incompatible with Oracle's PL/SQL. 

If no condition name nor SQLSTATE is specified in a `RAISE EXCEPTION` command, the default is to use `raise_exception` (`P0001`). If no message text is specified, the default is to use the condition name or SQLSTATE as message text. 

### Note

When specifying an error code by SQLSTATE code, you are not limited to the predefined error codes, but can select any error code consisting of five digits and/or upper-case ASCII letters, other than `00000`. It is recommended that you avoid throwing error codes that end in three zeroes, because these are category codes and can only be trapped by trapping the whole category. 

### 41.9.2. Checking Assertions #

The `ASSERT` statement is a convenient shorthand for inserting debugging checks into PL/pgSQL functions. 
    
    
    ASSERT _condition_ [ , _message_ ];
    

The _`condition`_ is a Boolean expression that is expected to always evaluate to true; if it does, the `ASSERT` statement does nothing further. If the result is false or null, then an `ASSERT_FAILURE` exception is raised. (If an error occurs while evaluating the _`condition`_ , it is reported as a normal error.) 

If the optional _`message`_ is provided, it is an expression whose result (if not null) replaces the default error message text “assertion failed”, should the _`condition`_ fail. The _`message`_ expression is not evaluated in the normal case where the assertion succeeds. 

Testing of assertions can be enabled or disabled via the configuration parameter `plpgsql.check_asserts`, which takes a Boolean value; the default is `on`. If this parameter is `off` then `ASSERT` statements do nothing. 

Note that `ASSERT` is meant for detecting program bugs, not for reporting ordinary error conditions. Use the `RAISE` statement, described above, for that. 

* * *

[Prev](plpgsql-transactions.md "41.8. Transaction Management") | [Up](plpgsql.md "Chapter 41. PL/pgSQL — SQL Procedural Language")|  [Next](plpgsql-trigger.md "41.10. Trigger Functions")  
---|---|---  
41.8. Transaction Management | [Home](index.md "PostgreSQL 17.5 Documentation")|  41.10. Trigger Functions
