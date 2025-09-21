44.8. Transaction Management  
---  
[Prev](plpython-subtransaction.md "44.7. Explicit Subtransactions") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")| Chapter 44. PL/Python — Python Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](plpython-util.md "44.9. Utility Functions")  
  
* * *

## 44.8. Transaction Management #

In a procedure called from the top level or an anonymous code block (`DO` command) called from the top level it is possible to control transactions. To commit the current transaction, call `plpy.commit()`. To roll back the current transaction, call `plpy.rollback()`. (Note that it is not possible to run the SQL commands `COMMIT` or `ROLLBACK` via `plpy.execute` or similar. It has to be done using these functions.) After a transaction is ended, a new transaction is automatically started, so there is no separate function for that. 

Here is an example: 
    
    
    CREATE PROCEDURE transaction_test1()
    LANGUAGE plpython3u
    AS $$
    for i in range(0, 10):
        plpy.execute("INSERT INTO test1 (a) VALUES (%d)" % i)
        if i % 2 == 0:
            plpy.commit()
        else:
            plpy.rollback()
    $$;
    
    CALL transaction_test1();
    

Transactions cannot be ended when an explicit subtransaction is active. 

* * *

[Prev](plpython-subtransaction.md "44.7. Explicit Subtransactions") | [Up](plpython.md "Chapter 44. PL/Python — Python Procedural Language")|  [Next](plpython-util.md "44.9. Utility Functions")  
---|---|---  
44.7. Explicit Subtransactions | [Home](index.md "PostgreSQL 17.5 Documentation")|  44.9. Utility Functions
