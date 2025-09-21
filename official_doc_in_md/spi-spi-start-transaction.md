SPI_start_transaction  
---  
[Prev](spi-spi-rollback.md "SPI_rollback") | [Up](spi-transaction.md "45.4. Transaction Management")| 45.4. Transaction Management| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](spi-visibility.md "45.5. Visibility of Data Changes")  
  
* * *

## SPI_start_transaction

SPI_start_transaction — obsolete function

## Synopsis
    
    
    void SPI_start_transaction(void)
    

## Description

`SPI_start_transaction` does nothing, and exists only for code compatibility with earlier PostgreSQL releases. It used to be required after calling `SPI_commit` or `SPI_rollback`, but now those functions start a new transaction automatically. 

* * *

[Prev](spi-spi-rollback.md "SPI_rollback") | [Up](spi-transaction.md "45.4. Transaction Management")|  [Next](spi-visibility.md "45.5. Visibility of Data Changes")  
---|---|---  
SPI_rollback | [Home](index.md "PostgreSQL 17.5 Documentation")|  45.5. Visibility of Data Changes
