45.5. Visibility of Data Changes  
---  
[Prev](spi-spi-start-transaction.md "SPI_start_transaction") | [Up](spi.md "Chapter 45. Server Programming Interface")| Chapter 45. Server Programming Interface| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-examples.md "45.6. Examples")  
  
* * *

## 45.5. Visibility of Data Changes #

The following rules govern the visibility of data changes in functions that use SPI (or any other C function): 

  * During the execution of an SQL command, any data changes made by the command are invisible to the command itself. For example, in: 
        
        INSERT INTO a SELECT * FROM a;
        

the inserted rows are invisible to the `SELECT` part. 

  * Changes made by a command C are visible to all commands that are started after C, no matter whether they are started inside C (during the execution of C) or after C is done. 

  * Commands executed via SPI inside a function called by an SQL command (either an ordinary function or a trigger) follow one or the other of the above rules depending on the read/write flag passed to SPI. Commands executed in read-only mode follow the first rule: they cannot see changes of the calling command. Commands executed in read-write mode follow the second rule: they can see all changes made so far. 

  * All standard procedural languages set the SPI read-write mode depending on the volatility attribute of the function. Commands of `STABLE` and `IMMUTABLE` functions are done in read-only mode, while commands of `VOLATILE` functions are done in read-write mode. While authors of C functions are able to violate this convention, it's unlikely to be a good idea to do so. 




The next section contains an example that illustrates the application of these rules. 

* * *

[Prev](spi-spi-start-transaction.md "SPI_start_transaction") | [Up](spi.md "Chapter 45. Server Programming Interface")|  [Next](spi-examples.md "45.6. Examples")  
---|---|---  
SPI_start_transaction | [Home](index.md "PostgreSQL 17.5 Documentation")|  45.6. Examples
