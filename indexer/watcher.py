"""
File watcher module for monitoring filesystem events.
Uses watchdog library to track file creation and modification events.
"""

import time
import threading
from pathlib import Path
from typing import Callable, List, Optional, Set
from datetime import datetime, timedelta

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent


class DebouncedEventHandler(FileSystemEventHandler):
    """
    Custom event handler with debouncing support.
    Prevents multiple rapid events from triggering multiple callbacks.
    """
    
    def __init__(
        self,
        callback: Callable[[str], None],
        valid_extensions: Set[str],
        max_file_size_bytes: int,
        debounce_seconds: float = 2.0
    ):
        """
        Initialize the event handler.
        
        Args:
            callback: Function to call with file path when debounced event occurs
            valid_extensions: Set of file extensions to monitor (e.g., {'.pdf', '.txt'})
            max_file_size_bytes: Maximum file size in bytes to process
            debounce_seconds: Seconds to wait before triggering callback
        """
        super().__init__()
        self.callback = callback
        self.valid_extensions = valid_extensions
        self.max_file_size_bytes = max_file_size_bytes
        self.debounce_seconds = debounce_seconds
        
        # Track pending events and their timers
        self.pending_events: dict[str, threading.Timer] = {}
        self.lock = threading.Lock()
    
    def should_process_file(self, file_path: str) -> bool:
        """
        Check if a file should be processed.
        
        Args:
            file_path: Path to the file to check
            
        Returns:
            True if file should be processed, False otherwise
        """
        path = Path(file_path)
        
        # Skip directories
        if not path.is_file():
            return False
        
        # Skip temp files (starting with ~ or .)
        if path.name.startswith('~') or path.name.startswith('.'):
            return False
        
        # Check file extension
        if path.suffix.lower() not in self.valid_extensions:
            return False
        
        # Check file size
        try:
            file_size = path.stat().st_size
            if file_size > self.max_file_size_bytes:
                return False
        except (OSError, FileNotFoundError):
            # File might not exist yet or be inaccessible
            return False
        
        return True
    
    def process_file(self, file_path: str) -> None:
        """
        Process the file after debounce delay.
        
        Args:
            file_path: Path to the file to process
        """
        # Remove from pending events
        with self.lock:
            if file_path in self.pending_events:
                del self.pending_events[file_path]
        
        # Double-check file still exists and should be processed
        if Path(file_path).exists() and self.should_process_file(file_path):
            try:
                self.callback(file_path)
            except Exception as e:
                print(f"Error in callback for {file_path}: {e}")
    
    def schedule_event(self, file_path: str) -> None:
        """
        Schedule or reschedule an event for debouncing.
        
        Args:
            file_path: Path to the file that triggered the event
        """
        with self.lock:
            # Cancel existing timer if present
            if file_path in self.pending_events:
                self.pending_events[file_path].cancel()
            
            # Create new timer
            timer = threading.Timer(self.debounce_seconds, self.process_file, [file_path])
            timer.daemon = True
            self.pending_events[file_path] = timer
            timer.start()
    
    def on_created(self, event: FileCreatedEvent) -> None:
        """
        Handle file creation events.
        
        Args:
            event: File creation event
        """
        if not event.is_directory:
            if self.should_process_file(event.src_path):
                self.schedule_event(event.src_path)
    
    def on_modified(self, event: FileModifiedEvent) -> None:
        """
        Handle file modification events.
        
        Args:
            event: File modification event
        """
        if not event.is_directory:
            if self.should_process_file(event.src_path):
                self.schedule_event(event.src_path)


class FileWatcher:
    """
    Main file watcher class that monitors directories for file changes.
    """
    
    def __init__(
        self,
        folders_to_watch: List[str],
        file_extensions: List[str],
        callback: Callable[[str], None],
        max_file_size_mb: float = 50.0,
        debounce_seconds: float = 2.0
    ):
        """
        Initialize the file watcher.
        
        Args:
            folders_to_watch: List of folder paths to monitor recursively
            file_extensions: List of file extensions to monitor (e.g., ['.pdf', '.txt'])
            callback: Function to call when a file is created or modified
            max_file_size_mb: Maximum file size in MB to process (default: 50)
            debounce_seconds: Seconds to wait before triggering callback (default: 2)
        """
        self.folders_to_watch = [str(Path(folder).expanduser().resolve()) for folder in folders_to_watch]
        self.valid_extensions = set(ext.lower() for ext in file_extensions)
        self.callback = callback
        self.max_file_size_bytes = int(max_file_size_mb * 1024 * 1024)
        self.debounce_seconds = debounce_seconds
        
        self.observer: Optional[Observer] = None
        self.event_handler: Optional[DebouncedEventHandler] = None
        self.is_watching = False
        
        # Validate folders exist
        self._validate_folders()
    
    def _validate_folders(self) -> None:
        """
        Validate that all folders to watch exist.
        Raises FileNotFoundError if any folder doesn't exist.
        """
        for folder in self.folders_to_watch:
            if not Path(folder).exists():
                raise FileNotFoundError(f"Folder does not exist: {folder}")
            if not Path(folder).is_dir():
                raise NotADirectoryError(f"Path is not a directory: {folder}")
    
    def start(self) -> None:
        """
        Start watching for file changes.
        """
        if self.is_watching:
            print("File watcher is already running")
            return
        
        # Create event handler
        self.event_handler = DebouncedEventHandler(
            callback=self.callback,
            valid_extensions=self.valid_extensions,
            max_file_size_bytes=self.max_file_size_bytes,
            debounce_seconds=self.debounce_seconds
        )
        
        # Create and start observer
        self.observer = Observer()
        
        # Schedule watches for each folder
        for folder in self.folders_to_watch:
            self.observer.schedule(self.event_handler, folder, recursive=True)
            print(f"Watching folder: {folder}")
        
        self.observer.start()
        self.is_watching = True
        print(f"File watcher started. Monitoring {len(self.folders_to_watch)} folder(s)")
        print(f"Valid extensions: {', '.join(sorted(self.valid_extensions))}")
        print(f"Max file size: {self.max_file_size_bytes // (1024*1024)} MB")
        print(f"Debounce delay: {self.debounce_seconds} seconds")
    
    def stop(self) -> None:
        """
        Stop watching for file changes.
        """
        if not self.is_watching:
            return
        
        # Cancel all pending timers
        if self.event_handler:
            with self.event_handler.lock:
                for timer in self.event_handler.pending_events.values():
                    timer.cancel()
                self.event_handler.pending_events.clear()
        
        # Stop the observer
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        self.is_watching = False
        print("File watcher stopped")
    
    def get_watched_folders(self) -> List[str]:
        """
        Get the list of folders currently being watched.
        
        Returns:
            List of folder paths
        """
        return self.folders_to_watch.copy()
    
    def get_valid_extensions(self) -> Set[str]:
        """
        Get the set of valid file extensions.
        
        Returns:
            Set of file extensions
        """
        return self.valid_extensions.copy()


def test_callback(file_path: str) -> None:
    """
    Simple test callback that prints the file path.
    
    Args:
        file_path: Path to the file that triggered the event
    """
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] File detected: {file_path}")


if __name__ == "__main__":
    """
    Simple test: Watch the current directory and print any detected files.
    Press Ctrl+C to stop.
    """
    import sys
    
    def on_file_detected(file_path: str) -> None:
        """Test callback for demo mode."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] 📄 File ready for indexing: {file_path}")
        
        # Get file info
        try:
            path = Path(file_path)
            size_bytes = path.stat().st_size
            size_kb = size_bytes / 1024
            print(f"    Size: {size_kb:.2f} KB")
            print(f"    Extension: {path.suffix}")
        except Exception as e:
            print(f"    Error reading file info: {e}")
    
    def main():
        """Run the file watcher in test mode."""
        # Get current directory
        current_dir = Path.cwd()
        
        # Common extensions to watch for testing
        test_extensions = [
            '.txt', '.md', '.pdf', '.docx', '.xlsx', 
            '.csv', '.jpg', '.jpeg', '.png'
        ]
        
        print("=" * 60)
        print("FILE WATCHER TEST MODE")
        print("=" * 60)
        print(f"Watching directory: {current_dir}")
        print(f"Extensions: {', '.join(test_extensions)}")
        print("Press Ctrl+C to stop")
        print("-" * 60)
        
        try:
            # Create and start the file watcher
            watcher = FileWatcher(
                folders_to_watch=[str(current_dir)],
                file_extensions=test_extensions,
                callback=on_file_detected,
                max_file_size_mb=50,
                debounce_seconds=2
            )
            
            watcher.start()
            
            # Keep the main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\nStopping file watcher...")
            watcher.stop()
            print("Test completed. Goodbye!")
            sys.exit(0)
        except Exception as e:
            print(f"\nError: {e}")
            sys.exit(1)
    
    main()




