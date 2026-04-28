"""
Orchestrator module for the File Indexer.
Coordinates the watcher, extractor, tagger, and database into a single pipeline.
"""

import os
import time
import threading
import logging
from pathlib import Path
from typing import List, Optional, Set
from datetime import datetime

from .config_loader import load_config
from .watcher import FileWatcher
from .extractor import extract_text
from .tagger import LLMTagger
from .database import FileDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FileIndexerOrchestrator:
    """
    Main orchestrator that manages the file indexing pipeline.
    Connects filesystem watching, text extraction, LLM tagging, and database storage.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the orchestrator with configuration.
        
        Args:
            config_path: Optional path to config.json (uses default if None)
        """
        # Load configuration
        self.config = load_config() if config_path is None else load_config(config_path)
        
        # Initialize database
        logger.info(f"Initializing database at: {self.config['database_path']}")
        self.db = FileDatabase(self.config['database_path'])
        
        # Initialize LLM tagger
        logger.info(f"Initializing LLM tagger with model: {self.config['model_path']}")
        self.tagger = LLMTagger(
            model_path=self.config['model_path'],
            vision_model_path=self.config.get('vision_model_path'),
            vision_projector_path=self.config.get('vision_projector_path'),  # NEW
            use_gpu=self.config.get('use_gpu', False),
            gpu_layers=self.config.get('gpu_layers', 0)
        )        

        # Check if tagger loaded successfully
        if not self.tagger.is_loaded():
            logger.warning("Text model not loaded! Some features may not work.")
        
        # Watcher will be initialized later
        self.watcher = None
        self.is_running = False
        self.scan_thread = None
        
        # Statistics
        self.stats = {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "changed": 0
        }
        
        # File extensions set for quick lookup
        self.valid_extensions = set(self.config['file_extensions'])
        
        logger.info(f"Orchestrator initialized. Watching {len(self.config['watched_folders'])} folders")
    
    def process_file(self, file_path: str) -> bool:
        """
        Process a single file through the entire pipeline.
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            True if successfully indexed, False otherwise
        """
        # Check if file still exists
        if not Path(file_path).exists():
            logger.debug(f"File no longer exists, skipping: {file_path}")
            self.stats["skipped"] += 1
            return False
        
        # Check file size
        try:
            file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
            if file_size_mb > self.config['max_file_size_mb']:
                logger.debug(f"File too large ({file_size_mb:.1f} MB > {self.config['max_file_size_mb']} MB): {file_path}")
                self.stats["skipped"] += 1
                return False
        except OSError as e:
            logger.error(f"Cannot access file size: {file_path} - {e}")
            self.stats["failed"] += 1
            return False
        
        # Check if file has changed
        try:
            if not self.db.file_changed(file_path):
                logger.debug(f"File unchanged, skipping: {file_path}")
                self.stats["skipped"] += 1
                return False
        except Exception as e:
            logger.error(f"Error checking file change status: {file_path} - {e}")
            # Continue processing - better to re-index than miss changes
        
        # Extract text content
        try:
            logger.debug(f"Extracting text from: {file_path}")
            extracted_text = extract_text(file_path)
            
            if not extracted_text and Path(file_path).suffix.lower() not in ['.jpg', '.jpeg', '.png']:
                logger.warning(f"No text extracted from: {file_path}")
                # Still try to process with empty text
        except Exception as e:
            logger.error(f"Text extraction failed for {file_path}: {e}")
            self.stats["failed"] += 1
            return False
        
        # Tag with LLM
        try:
            logger.debug(f"Tagging file: {file_path}")
            if extracted_text == "IMAGE_FILE":
                metadata = self.tagger.tag_image_file(file_path)
            else:
                metadata = self.tagger.tag_text_file(file_path, extracted_text)
        except Exception as e:
            logger.error(f"LLM tagging failed for {file_path}: {e}")
            self.stats["failed"] += 1
            return False
        
        # Store in database
        try:
            self.db.upsert_file(file_path, metadata)
            logger.info(f"[INDEXED] {Path(file_path).name} — {metadata.get('document_type', 'unknown')}")
            self.stats["processed"] += 1
            return True
        except Exception as e:
            logger.error(f"Database upsert failed for {file_path}: {e}")
            self.stats["failed"] += 1
            return False
    
    def index_existing_files(self) -> None:
        """
        Scan all watched folders and index existing files.
        Runs once at startup to catch files created while the indexer was off.
        """
        logger.info("Starting scan of existing files...")
        start_time = time.time()
        
        # Collect all files to process
        files_to_process = []
        total_files_found = 0
        
        for folder in self.config['watched_folders']:
            folder_path = Path(folder)
            if not folder_path.exists():
                logger.warning(f"Watched folder does not exist: {folder}")
                continue
            
            # Walk through directory
            try:
                for root, dirs, files in os.walk(folder_path):
                    # Skip hidden directories
                    dirs[:] = [d for d in dirs if not d.startswith('.')]
                    
                    for file in files:
                        # Skip hidden files
                        if file.startswith('.') or file.startswith('~'):
                            continue
                        
                        file_path = Path(root) / file
                        extension = file_path.suffix.lower()
                        
                        # Check if extension is valid
                        if extension in self.valid_extensions:
                            total_files_found += 1
                            
                            # Check file size
                            try:
                                if file_path.stat().st_size / (1024 * 1024) <= self.config['max_file_size_mb']:
                                    files_to_process.append(str(file_path))
                                else:
                                    logger.debug(f"Skipping large file: {file_path}")
                            except OSError:
                                continue
            except Exception as e:
                logger.error(f"Error scanning folder {folder}: {e}")
        
        logger.info(f"[SCAN] Found {total_files_found} total files, {len(files_to_process)} within size limits")
        
        # Process files
        if files_to_process:
            logger.info(f"Processing {len(files_to_process)} files...")
            for i, file_path in enumerate(files_to_process, 1):
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(files_to_process)} files processed")
                
                self.process_file(file_path)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Initial scan complete in {elapsed_time:.2f} seconds")
        logger.info(f"Stats: {self.stats['processed']} indexed, {self.stats['skipped']} skipped, {self.stats['failed']} failed")
    
    def _watcher_callback(self, file_path: str) -> None:
        """
        Callback for the file watcher.
        Processes files as they are created or modified.
        
        Args:
            file_path: Path to the changed file
        """
        if not self.is_running:
            return
        
        # Process the file
        self.process_file(file_path)
    
    def start(self) -> None:
        """
        Start the indexing service.
        - Indexes existing files in background
        - Starts file watcher for new/changed files
        """
        if self.is_running:
            logger.warning("Orchestrator is already running")
            return
        
        self.is_running = True
        logger.info("=" * 60)
        logger.info("FILE INDEXER STARTING")
        logger.info("=" * 60)
        
        # Reset stats for this run
        self.stats = {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "changed": 0
        }
        
        # Start initial scan in background thread
        logger.info("Starting background scan of existing files...")
        self.scan_thread = threading.Thread(target=self.index_existing_files, daemon=True)
        self.scan_thread.start()
        
        # Wait a moment for scan to start
        time.sleep(1)
        
        # Start file watcher
        try:
            logger.info("Starting file watcher...")
            self.watcher = FileWatcher(
                folders_to_watch=self.config['watched_folders'],
                file_extensions=self.config['file_extensions'],
                callback=self._watcher_callback,
                max_file_size_mb=self.config['max_file_size_mb'],
                debounce_seconds=2.0
            )
            
            self.watcher.start()
            logger.info(f"File indexer is now running. Watching {len(self.config['watched_folders'])} folders")
            logger.info("Press Ctrl+C to stop")

            try:
                while self.is_running:
                    time.sleep(0.5)
            except KeyboardInterrupt:
                self.stop()
            
        except Exception as e:
            logger.error(f"Failed to start file watcher: {e}")
            self.stop()
            raise
    
    def stop(self) -> None:
        """
        Stop the indexing service gracefully.
        """
        if not self.is_running:
            return
        
        logger.info("Stopping file indexer...")
        self.is_running = False
        
        # Stop watcher
        if self.watcher:
            self.watcher.stop()
            self.watcher = None
        
        # Wait for scan thread to complete
        if self.scan_thread and self.scan_thread.is_alive():
            logger.info("Waiting for initial scan to complete...")
            self.scan_thread.join(timeout=30)
        
        logger.info("File indexer stopped")
        self._print_final_stats()
    
    def _print_final_stats(self) -> None:
        """
        Print final statistics about the indexing run.
        """
        logger.info("=" * 60)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Files indexed: {self.stats['processed']}")
        logger.info(f"Files skipped (unchanged/size): {self.stats['skipped']}")
        logger.info(f"Files failed: {self.stats['failed']}")
        
        # Get database stats
        try:
            db_stats = self.db.get_all_stats()
            logger.info(f"Total files in database: {db_stats['total_files']}")
            logger.info(f"Document types: {len(db_stats['by_document_type'])}")
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
    
    def cleanup_missing_files(self) -> int:
        """
        Remove database records for files that no longer exist on disk.

        Returns:
            Number of records deleted
        """
        logger.info("Starting cleanup of missing files...")

        try:
            stats_before = self.db.get_all_stats()
            total_before = stats_before['total_files']
            logger.info(f"Total files in database before cleanup: {total_before}")

            deleted_count = self.db.delete_missing_files()

            stats_after = self.db.get_all_stats()
            total_after = stats_after['total_files']

            logger.info(f"Cleanup complete: {deleted_count} records deleted")
            logger.info(f"Database now contains {total_after} files (was {total_before})")

            if hasattr(self, 'show_notification'):
                self.show_notification(
                    "File Indexer Cleanup",
                    f"Removed {deleted_count} missing files\nDatabase: {total_after} files"
                )

            return deleted_count

        except Exception as e:
            logger.error(f"Cleanup failed: {e}", exc_info=True)
            raise

    def reindex_all(self, confirm: bool = True) -> int:
        """
        Clear all records and re-index all existing files from scratch.
        Useful when updating models or fixing database issues.

        Args:
            confirm: If True, prompt user for confirmation before proceeding
        
        Returns:
            Number of files reindexed
        """
        logger.info("=" * 60)
        logger.info("STARTING FULL REINDEX")
        logger.info("=" * 60)

        try:
            stats_before = self.db.get_all_stats()
            total_before = stats_before['total_files']
            logger.info(f"Files in database before reindex: {total_before}")

            if confirm:
                response = input(f"\nThis will delete {total_before} records and re-index all files. Continue? (y/N): ")
                if response.lower() != 'y':
                    logger.info("Reindex cancelled by user")
                    return 0

            logger.info("Clearing database...")
            deleted_count = self.db.clear_all()
            logger.info(f"Cleared {deleted_count} records from database")

            self.stats = {"processed": 0, "skipped": 0, "failed": 0, "changed": 0}

            logger.info("Starting fresh index of all files...")
            self.index_existing_files()

            stats_after = self.db.get_all_stats()
            total_after = stats_after['total_files']

            logger.info("=" * 60)
            logger.info(f"REINDEX COMPLETE: {total_after} files indexed")
            logger.info(f"Session stats: {self.stats['processed']} processed, "
                        f"{self.stats['failed']} failed")
            logger.info("=" * 60)

            if hasattr(self, 'show_notification'):
                self.show_notification(
                    "File Indexer Reindex Complete",
                    f"Reindexed {total_after} files\nFailed: {self.stats['failed']}"
                )    

            return total_after

        except Exception as e:
            logger.error(f"Reindex failed: {e}", exc_info=True)
            raise

    def reindex_file(self, file_path: str) -> bool:
        """
        Force re-index a single file regardless of hash.
        Useful when updating models or fixing metadata.

        Args:
            file_path: Path to the file to reindex

        Returns:
            True if successfully reindexed, False otherwise
        """
        logger.info(f"Forcing reindex of file: {file_path}")

        if not Path(file_path).exists():
            logger.error(f"File does not exist: {file_path}")
            return False

        self.db.delete_file(file_path)
        success = self.process_file(file_path)

        if success:
            logger.info(f"Successfully reindexed: {file_path}")
        else:
            logger.error(f"Failed to reindex: {file_path}")

        return success

    def get_detailed_stats(self) -> dict:
        """
        Get detailed statistics including database info and system status.

        Returns:
            Dictionary with comprehensive statistics
        """
        db_stats = self.db.get_all_stats()
        extensions = db_stats.get("by_extension", {})
        recent = self.db.get_recent_files(limit=5)

        return {
            "database": db_stats,
            "extensions": extensions,
            "recent_files": recent,
            "orchestrator": {
                "is_running": self.is_running,
                "is_paused": hasattr(self, 'is_paused') and self.is_paused,
                "session_stats": self.stats.copy(),
                "watcher_active": bool(self.watcher and getattr(self.watcher, 'is_watching', False))
            },
            "model": self.tagger.get_model_info() if self.tagger else {},
            "config": {
                "watched_folders": len(self.config.get('watched_folders', [])),
                "file_extensions": len(self.config.get('file_extensions', [])),
                "max_file_size_mb": self.config.get('max_file_size_mb', 50),
                "use_gpu": self.config.get('use_gpu', False)
            }
        }

    def get_status(self) -> dict:
        """
        Get current status of the orchestrator.
        
        Returns:
            Dictionary with status information
        """
        db_stats = {}
        try:
            db_stats = self.db.get_all_stats()
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
        
        return {
            "is_running": self.is_running,
            "watcher_active": self.watcher is not None and self.watcher.is_watching,
            "stats": self.stats.copy(),
            "database": db_stats,
            "config": {
                "watched_folders": len(self.config['watched_folders']),
                "file_extensions": len(self.config['file_extensions']),
                "model_loaded": self.tagger.is_loaded(),
                "vision_model_loaded": self.tagger.is_vision_loaded(),
                "max_file_size_mb": self.config['max_file_size_mb']
            }
        }


def main():
    """Test the orchestrator standalone."""
    import signal
    import sys
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\n\nReceived interrupt signal...")
        orchestrator.stop()
        sys.exit(0)
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create orchestrator
        logger.info("Initializing File Indexer Orchestrator...")
        orchestrator = FileIndexerOrchestrator()
        
        # Start orchestrator
        orchestrator.start()
        
        # Keep main thread alive
        while orchestrator.is_running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        orchestrator.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()



