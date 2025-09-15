#!/usr/bin/env python3
"""
Initialize the documents database with proper schema
"""
import duckdb
import sys
from pathlib import Path

def init_documents_database():
    """Initialize the documents database with the proper schema"""
    db_path = Path('data/documents.duckdb')
    
    print(f"Initializing documents database at {db_path}")
    
    # Create the database connection
    con = duckdb.connect(str(db_path))
    
    # Create the documents table
    con.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            symbol_id INTEGER PRIMARY KEY,
            symbol_name TEXT NOT NULL,
            symbol_type TEXT NOT NULL,
            layer INTEGER NOT NULL DEFAULT 0,
            content TEXT NOT NULL,
            summary TEXT,
            dependencies TEXT,
            related_symbols TEXT,
            quality_score REAL DEFAULT 0.0,
            quality_level TEXT DEFAULT 'UNKNOWN',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create the processing log table
    con.execute("""
        CREATE TABLE IF NOT EXISTS processing_log (
            batch_id INTEGER PRIMARY KEY,
            symbol_ids TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            processed_count INTEGER DEFAULT 0,
            error_message TEXT,
            quality_score REAL DEFAULT 0.0,
            context_reset BOOLEAN DEFAULT FALSE
        )
    """)
    
    con.commit()
    con.close()
    
    print("✅ Documents database initialized successfully")
    print("   - documents table created")
    print("   - processing_log table created")

if __name__ == "__main__":
    init_documents_database()