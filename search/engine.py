"""Search engine module for file indexer - handles queries and file operations."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from indexer.database import FileDatabase


class SearchEngine:
    """Search engine for querying indexed files and opening results."""
    
    def __init__(self, db_path: str = "file_index.db"):
        """Initialize search engine with database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.database = FileDatabase(db_path)
    
    def search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for files matching query.
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing file metadata and search results
        """
        # Clean the query
        query = query.strip()
        if not query:
            return []
        
        # Perform database search
        results = self.database.search(query, limit)
        
        # Process each result
        processed_results = []
        for result in results:
            # Check if file still exists on disk
            file_path = result.get('file_path', '')
            exists = os.path.exists(file_path) if file_path else False
            
            # Parse JSON fields back to Python objects
            result['tags'] = self._parse_json_field(result.get('tags', '[]'))
            result['keywords'] = self._parse_json_field(result.get('keywords', '[]'))
            result['people_mentioned'] = self._parse_json_field(
                result.get('people_mentioned', '[]')
            )
            
            # Add existence flag
            result['exists'] = exists
            
            processed_results.append(result)
        
        # Sort: existing files first, then missing files
        processed_results.sort(key=lambda x: (not x['exists'], x.get('file_name', '')))
        
        return processed_results
    
    def open_file(self, file_path: str) -> bool:
        """Open file with system default application.
        
        Args:
            file_path: Path to file to open
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            return False
        
        try:
            if sys.platform == "win32":
                # Windows
                os.startfile(file_path)
            elif sys.platform == "darwin":
                # macOS
                subprocess.Popen(["open", file_path])
            else:
                # Linux and other Unix-like
                subprocess.Popen(["xdg-open", file_path])
            return True
        except Exception as e:
            print(f"Error opening file {file_path}: {e}")
            return False
    
    def open_folder(self, file_path: str) -> bool:
        """Open folder containing file and highlight/select the file.
        
        Args:
            file_path: Path to file whose folder should be opened
            
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            return False
        
        folder_path = os.path.dirname(file_path)
        if not os.path.exists(folder_path):
            return False
        
        try:
            if sys.platform == "win32":
                # Windows: open explorer with file selected
                subprocess.Popen(["explorer", "/select,", file_path])
            elif sys.platform == "darwin":
                # macOS: open finder with file selected
                subprocess.Popen(["open", "-R", file_path])
            else:
                # Linux: open folder with default file manager
                subprocess.Popen(["xdg-open", folder_path])
            return True
        except Exception as e:
            print(f"Error opening folder for {file_path}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get indexer statistics.
    
        Returns:
            Dictionary containing statistics about indexed files
        """
        stats = {
            'total_files': 0,
            'missing_files': 0,
            'by_type': {},
            'last_indexed': None
        }
    
        try:
            with self.database._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT file_path, extension, indexed_at
                    FROM indexed_files
                """)
                files = cursor.fetchall()
            
                stats['total_files'] = len(files)
            
                # Count by file type and check existence
                missing_count = 0
                for row in files:
                    if not os.path.exists(row['file_path']):
                        missing_count += 1
                    ext = row['extension'].lower() if row['extension'] else 'unknown'
                    stats['by_type'][ext] = stats['by_type'].get(ext, 0) + 1
            
                stats['missing_files'] = missing_count
            
                cursor.execute("SELECT MAX(indexed_at) FROM indexed_files")
                last_indexed = cursor.fetchone()[0]
                if last_indexed:
                    stats['last_indexed'] = last_indexed
                
        except Exception as e:
            print(f"Error getting stats: {e}")
    
        return stats
    
    def get_file_details(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific file.
    
        Args:
            file_path: Path to file to get details for
        
        Returns:
            Dictionary with file details or None if not found
        """
        try:
            with self.database._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, file_path, file_name, extension, summary, 
                           document_type, tags, keywords, people_mentioned, 
                           date_hint, indexed_at, file_hash
                    FROM indexed_files 
                    WHERE file_path = ?
                """, (file_path,))
            
                result = cursor.fetchone()
                if not result:
                    return None
            
                # sqlite3.Row is already dict-like, convert directly
                details = dict(result)
                details['tags'] = self._parse_json_field(details.get('tags', '[]'))
                details['keywords'] = self._parse_json_field(details.get('keywords', '[]'))
                details['people_mentioned'] = self._parse_json_field(
                    details.get('people_mentioned', '[]')
                )
                details['exists'] = os.path.exists(file_path)
            
                return details
            
        except Exception as e:
            print(f"Error getting file details: {e}")
            return None
    
    def _parse_json_field(self, json_str) -> list:
        """Parse JSON string field to Python list.
        
        Args:
            json_str: JSON string representation of list
            
        Returns:
            Python list, empty list if parsing fails
        """
        if isinstance(json_str, list):
            return json_str
        if not json_str or json_str == 'null':
            return []
        try:
            parsed = json.loads(json_str)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    
    def close(self):
        """Close database connection."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure connection is closed."""
        self.close()



