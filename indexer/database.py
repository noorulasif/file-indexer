"""
Database module for the File Indexer.
Handles SQLite operations including schema management, CRUD operations, and FTS5 search.
"""

import sqlite3
import json
import hashlib
import os

from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from contextlib import contextmanager

import logging
logger = logging.getLogger(__name__)

class FileDatabase:
    """
    Database manager for indexed files with FTS5 full-text search support.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize the database connection and create schema if needed.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self._init_database()
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager for database connections.
        Ensures proper cleanup and foreign key enforcement.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_database(self) -> None:
        """
        Create the database schema if it doesn't exist.
        Creates the main table and FTS5 virtual table.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Create main indexed_files table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indexed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE NOT NULL,
                    file_name TEXT NOT NULL,
                    extension TEXT,
                    summary TEXT,
                    document_type TEXT,
                    tags TEXT,               -- JSON string
                    keywords TEXT,           -- JSON string
                    people_mentioned TEXT,   -- JSON string
                    date_hint TEXT,
                    indexed_at TEXT NOT NULL,
                    file_hash TEXT NOT NULL
                )
            """)
            
            # Create indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_path 
                ON indexed_files(file_path)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_document_type 
                ON indexed_files(document_type)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_indexed_at 
                ON indexed_files(indexed_at)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_extension 
                ON indexed_files(extension)
            """)
            
            # Create FTS5 virtual table for full-text search
            # Note: FTS5 tables need to be created separately from regular tables
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS files_fts 
                USING fts5(
                    file_name,
                    summary,
                    tags,
                    keywords,
                    document_type,
                    content=indexed_files,
                    content_rowid=id
                )
            """)
            
            # Create triggers to keep FTS table in sync
            # Trigger for INSERT
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS files_fts_insert 
                AFTER INSERT ON indexed_files
                BEGIN
                    INSERT INTO files_fts(
                        rowid, file_name, summary, tags, keywords, document_type
                    ) VALUES (
                        new.id, new.file_name, new.summary, 
                        new.tags, new.keywords, new.document_type
                    );
                END
            """)
            
            # Trigger for UPDATE
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS files_fts_update AFTER UPDATE ON indexed_files BEGIN
                    DELETE FROM files_fts WHERE rowid = old.id;
                    INSERT INTO files_fts(rowid, file_name, summary, tags, keywords, document_type)
                    VALUES (new.id, new.file_name, new.summary, new.tags, new.keywords, new.document_type);
                END
            """)
            
            # Trigger for DELETE
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS files_fts_delete 
                AFTER DELETE ON indexed_files
                BEGIN
                    DELETE FROM files_fts WHERE rowid = old.id;
                END
            """)
    
    def _compute_file_hash(self, file_path: str) -> str:
        """
        Compute MD5 hash of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MD5 hash as hexadecimal string
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                # Read in chunks to handle large files efficiently
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except (IOError, OSError) as e:
            print(f"Error hashing file {file_path}: {e}")
            return ""
    
    def upsert_file(self, file_path: str, metadata: Dict[str, Any]) -> None:
        """
        Insert or update a file record in the database.
        
        Args:
            file_path: Absolute path to the file
            metadata: Dictionary containing metadata from the LLM tagger
        """
        file_path = str(Path(file_path).absolute())
        file_name = Path(file_path).name
        extension = Path(file_path).suffix.lower()
        
        # Compute file hash
        file_hash = self._compute_file_hash(file_path)
        if not file_hash:
            raise ValueError(f"Could not compute hash for file: {file_path}")
        
        # Prepare metadata fields with defaults
        summary = metadata.get("summary", "")
        document_type = metadata.get("document_type", "other")
        tags = json.dumps(metadata.get("tags", []))
        keywords = json.dumps(metadata.get("keywords", []))
        people_mentioned = json.dumps(metadata.get("people_mentioned", []))
        date_hint = metadata.get("date_hint")
        indexed_at = datetime.now().isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if file exists
            cursor.execute("SELECT id, file_hash FROM indexed_files WHERE file_path = ?", (file_path,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                cursor.execute("""
                    UPDATE indexed_files 
                    SET file_name = ?,
                        extension = ?,
                        summary = ?,
                        document_type = ?,
                        tags = ?,
                        keywords = ?,
                        people_mentioned = ?,
                        date_hint = ?,
                        indexed_at = ?,
                        file_hash = ?
                    WHERE file_path = ?
                """, (
                    file_name, extension, summary, document_type, 
                    tags, keywords, people_mentioned, date_hint, 
                    indexed_at, file_hash, file_path
                ))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO indexed_files (
                        file_path, file_name, extension, summary, document_type,
                        tags, keywords, people_mentioned, date_hint, indexed_at, file_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_path, file_name, extension, summary, document_type,
                    tags, keywords, people_mentioned, date_hint, indexed_at, file_hash
                ))
    
    def is_indexed(self, file_path: str) -> bool:
        """
        Check if a file path exists in the database.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file exists in database
        """
        file_path = str(Path(file_path).absolute())
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM indexed_files WHERE file_path = ? LIMIT 1", 
                (file_path,)
            )
            return cursor.fetchone() is not None
    
    def file_changed(self, file_path: str) -> bool:
        """
        Check if a file has changed since it was last indexed.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file doesn't exist, hash doesn't match, or file not indexed
        """
        file_path = str(Path(file_path).absolute())
        
        # Check if file exists on disk
        if not Path(file_path).exists():
            return True
        
        # Compute current hash
        current_hash = self._compute_file_hash(file_path)
        if not current_hash:
            return True  # Can't compute hash, treat as changed
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT file_hash FROM indexed_files WHERE file_path = ?", 
                (file_path,)
            )
            result = cursor.fetchone()
            
            if not result:
                return True  # Not indexed at all
            
            stored_hash = result["file_hash"]
            return current_hash != stored_hash
    
    def search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Perform full-text search using FTS5.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries with search results
        """
        if not query.strip():
            return []
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Use FTS5 with BM25 ranking
            # The MATCH query searches across all indexed columns
            try:
                cursor.execute("""
                    SELECT 
                        f.rowid,
                        f.file_name,
                        f.summary,
                        f.document_type,
                        f.tags,
                        f.keywords,
                        i.file_path,
                        i.date_hint,
                        i.indexed_at,
                        i.extension,
                        i.people_mentioned,
                        rank               -- FTS5 relevance score (lower is better)
                    FROM files_fts f
                    JOIN indexed_files i ON f.rowid = i.id
                    WHERE files_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                """, (query, limit))
            except sqlite3.OperationalError:
                return []  # User typed invalid FTS5 syntax (e.g. bare quotes)

            results = []
            for row in cursor.fetchall():
                # Parse JSON fields
                try:
                    tags = json.loads(row["tags"]) if row["tags"] else []
                except (json.JSONDecodeError, TypeError):
                    tags = []
                
                try:
                    keywords = json.loads(row["keywords"]) if row["keywords"] else []
                except (json.JSONDecodeError, TypeError):
                    keywords = []
                
                try:
                    people_mentioned = json.loads(row["people_mentioned"]) if row["people_mentioned"] else []
                except (json.JSONDecodeError, TypeError):
                    people_mentioned = []
                
                results.append({
                    "file_path": row["file_path"],
                    "file_name": row["file_name"],
                    "extension": row["extension"],
                    "summary": row["summary"],
                    "document_type": row["document_type"],
                    "tags": tags,
                    "keywords": keywords,
                    "people_mentioned": people_mentioned,
                    "date_hint": row["date_hint"],
                    "indexed_at": row["indexed_at"],
                    "search_rank": row["rank"]  # Lower is better
                })
            
            return results
    
    def get_all_stats(self) -> Dict[str, Any]:
        """
        Get statistics about indexed files.
        
        Returns:
            Dictionary with total_files, by_type, and last_indexed
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Total files
            cursor.execute("SELECT COUNT(*) as count FROM indexed_files")
            total_files = cursor.fetchone()["count"]
            
            # Files by document type
            cursor.execute("""
                SELECT document_type, COUNT(*) as count 
                FROM indexed_files 
                WHERE document_type IS NOT NULL AND document_type != ''
                GROUP BY document_type 
                ORDER BY count DESC
            """)
            by_type = {row["document_type"]: row["count"] for row in cursor.fetchall()}
            
            # Files by extension
            cursor.execute("""
                SELECT extension, COUNT(*) as count 
                FROM indexed_files 
                WHERE extension IS NOT NULL
                GROUP BY extension 
                ORDER BY count DESC
                LIMIT 10
            """)
            by_extension = {row["extension"]: row["count"] for row in cursor.fetchall()}
            
            # Last indexed time
            cursor.execute("""
                SELECT MAX(indexed_at) as last_indexed 
                FROM indexed_files
            """)
            last_indexed = cursor.fetchone()["last_indexed"]
            
            # Total unique tags (across all files)
            cursor.execute("""
                SELECT tags FROM indexed_files WHERE tags IS NOT NULL
            """)
            all_tags = set()
            for row in cursor.fetchall():
                try:
                    tags = json.loads(row["tags"])
                    all_tags.update(tags)
                except (json.JSONDecodeError, TypeError):
                    pass
            
            return {
                "total_files": total_files,
                "by_document_type": by_type,
                "by_extension": by_extension,
                "last_indexed": last_indexed,
                "unique_tags_count": len(all_tags),
                "unique_tags_sample": list(all_tags)[:20]  # Show first 20 tags
            }
    
    def delete_missing_files(self) -> int:
        """
        Delete records for files that no longer exist on disk.
        
        Returns:
            Number of records deleted
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all indexed files
            cursor.execute("SELECT id, file_path FROM indexed_files")
            all_files = cursor.fetchall()
            
            deleted_count = 0
            for row in all_files:
                file_path = row["file_path"]
                if not Path(file_path).exists():
                    # Delete the record (FTS will be updated via trigger)
                    cursor.execute("DELETE FROM indexed_files WHERE id = ?", (row["id"],))
                    deleted_count += 1
            
            return deleted_count
    
    def get_file_by_path(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single file record by its path.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with file metadata or None if not found
        """
        file_path = str(Path(file_path).absolute())
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM indexed_files WHERE file_path = ?
            """, (file_path,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Parse JSON fields
            result = dict(row)
            result["tags"] = json.loads(result["tags"]) if result["tags"] else []
            result["keywords"] = json.loads(result["keywords"]) if result["keywords"] else []
            result["people_mentioned"] = json.loads(result["people_mentioned"]) if result["people_mentioned"] else []
            
            return result
    
    def get_recent_files(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recently indexed files.
        
        Args:
            limit: Maximum number of files to return
            
        Returns:
            List of file metadata dictionaries
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    file_path, file_name, extension, summary, 
                    document_type, date_hint, indexed_at
                FROM indexed_files
                ORDER BY indexed_at DESC
                LIMIT ?
            """, (limit,))
            
            results = []
            for row in cursor.fetchall():
                results.append(dict(row))
            
            return results
    
    def search_by_type(self, document_type: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search files by document type.
        
        Args:
            document_type: Type of document to filter by
            limit: Maximum number of results
            
        Returns:
            List of file metadata dictionaries
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    file_path, file_name, extension, summary, 
                    document_type, date_hint, indexed_at
                FROM indexed_files
                WHERE document_type = ?
                ORDER BY indexed_at DESC
                LIMIT ?
            """, (document_type, limit))
            
            results = []
            for row in cursor.fetchall():
                results.append(dict(row))
            
            return results
    
    def vacuum(self) -> None:
        """
        Optimize the database by running VACUUM.
        Rebuilds the database file, reclaiming unused space.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            conn.isolation_level = None  # autocommit mode
            conn.execute("VACUUM")
        finally:
            conn.close()
    
    def optimize_fts(self) -> None:
        """
        Optimize the FTS5 index for better search performance.
        """
        with self._get_connection() as conn:
            conn.execute("INSERT INTO files_fts(files_fts) VALUES('optimize')")

    def clear_all(self) -> int:
        """
        Clear ALL records from the database.
        Useful for full reindexing operations.
    
        Returns:
            Number of records deleted
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
        
            # Get count before deletion
            cursor.execute("SELECT COUNT(*) as count FROM indexed_files")
            count = cursor.fetchone()["count"]
        
            # Delete all records (FTS will be updated via triggers)
            cursor.execute("DELETE FROM indexed_files")
        
            logger.info(f"Cleared {count} records from database")

        # Vacuum to reclaim space
        self.vacuum()
        return count

    # Add to database.py
    def delete_file(self, file_path: str) -> bool:
        file_path = str(Path(file_path).absolute())
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM indexed_files WHERE file_path = ?", (file_path,))
            return cursor.rowcount > 0

def main():
    """Test the database module with sample operations."""
    import tempfile
    
    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    print(f"Testing database at: {db_path}")
    print("=" * 60)
    
    # Initialize database
    db = FileDatabase(db_path)
    print("✅ Database initialized")
    
    # Test upsert
    print("\n📝 Testing insert...")
    test_metadata = {
        "summary": "Test invoice document for office supplies",
        "document_type": "invoice",
        "tags": ["invoice", "office", "purchase"],
        "keywords": ["laptop", "mouse", "invoice", "payment"],
        "people_mentioned": ["John Smith"],
        "date_hint": "2024-03",
        "file_name": "test_invoice.pdf"
    }
    
    test_path = "/test/path/invoice.pdf"
    db.upsert_file(test_path, test_metadata)
    print("✅ Inserted test file")
    
    # Test is_indexed
    print("\n🔍 Testing is_indexed...")
    is_indexed = db.is_indexed(test_path)
    print(f"File indexed: {is_indexed}")
    
    # Test search
    print("\n🔎 Testing search...")
    results = db.search("invoice laptop")
    print(f"Search results: {len(results)} found")
    for result in results:
        print(f"  - {result['file_name']} (rank: {result['search_rank']:.2f})")
        print(f"    Summary: {result['summary']}")
    
    # Test stats
    print("\n📊 Testing stats...")
    stats = db.get_all_stats()
    print(f"Total files: {stats['total_files']}")
    print(f"By type: {stats['by_document_type']}")
    print(f"Last indexed: {stats['last_indexed']}")
    
    # Test get_recent_files
    print("\n📁 Testing get_recent_files...")
    recent = db.get_recent_files(limit=5)
    print(f"Recent files: {len(recent)}")
    
    # Test search_by_type
    print("\n🏷️ Testing search_by_type...")
    invoices = db.search_by_type("invoice")
    print(f"Invoices found: {len(invoices)}")
    
    # Test delete_missing_files
    print("\n🗑️ Testing delete_missing_files...")
    deleted = db.delete_missing_files()
    print(f"Deleted {deleted} missing files")
    
    # Test file_changed
    print("\n🔄 Testing file_changed...")
    changed = db.file_changed(test_path)
    print(f"File changed: {changed} (expected True since file doesn't exist)")
    
    # Clean up
    Path(db_path).unlink()
    print("\n🧹 Test database cleaned up")
    print("✅ All tests passed!")


if __name__ == "__main__":
    main()



