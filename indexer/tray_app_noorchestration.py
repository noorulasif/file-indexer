"""
System tray application for the File Indexer.
Provides a GUI interface to control the indexing service.
"""

import sys
import threading
import subprocess
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

try:
    import pystray
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("Error: pystray and Pillow are required. Install with: pip install pystray Pillow")
    sys.exit(1)

from .orchestrator import FileIndexerOrchestrator


class TrayApp:
    """
    System tray application that manages the file indexer.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the tray application.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.config_path = config_path
        self.orchestrator: Optional[FileIndexerOrchestrator] = None
        self.orchestrator_thread: Optional[threading.Thread] = None
        self.is_paused = False
        self.icon: Optional[pystray.Icon] = None
        self.menu_items = {}
        
        # Create the icon
        self.icon_image = self._create_icon_image()
        
        # Build the menu
        self._build_menu()
        
        # Set up status update timer
        self.status_update_timer = None
    
    def _create_icon_image(self) -> Image.Image:
        """
        Create a programmatic icon image with the letter 'F'.
        
        Returns:
            PIL Image object
        """
        # Create a 64x64 image with a dark blue background
        size = 64
        image = Image.new('RGB', (size, size), color=(33, 150, 243))  # Material blue
        
        # Get drawing context
        draw = ImageDraw.Draw(image)
        
        # Try to load a font, fall back to default if not available
        try:
            # Try to use a larger system font
            font = ImageFont.truetype("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", 40)
        except (IOError, OSError):
            try:
                # Windows font
                font = ImageFont.truetype("arial.ttf", 40)
            except (IOError, OSError):
                # Default font
                font = ImageFont.load_default()
        
        # Draw the letter 'F' in white
        # Get text bounding box
        bbox = draw.textbbox((0, 0), "F", font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Center the text
        x = (size - text_width) // 2
        y = (size - text_height) // 2 - 5  # Slight adjustment
        
        draw.text((x, y), "F", fill=(255, 255, 255), font=font)
        
        return image
    
    def _build_menu(self) -> None:
        """
        Build the system tray menu structure.
        """
        # Create menu items
        self.menu_items = {
            "title": pystray.MenuItem(
                "File Indexer",
                None,
                enabled=False  # Grayed out / not clickable
            ),
            "separator1": pystray.Menu.SEPARATOR,
            "status": pystray.MenuItem(
                "Status: Initializing...",
                None,
                enabled=False
            ),
            "separator2": pystray.Menu.SEPARATOR,
            "pause_resume": pystray.MenuItem(
                "Pause Indexing",
                self._toggle_pause,
                enabled=True
            ),
            "separator3": pystray.Menu.SEPARATOR,
            "open_search": pystray.MenuItem(
                "Open Search App",
                self._open_search_app,
                enabled=True
            ),
            "view_stats": pystray.MenuItem(
                "View Stats",
                self._show_stats,
                enabled=True
            ),
            "separator4": pystray.Menu.SEPARATOR,
            "quit": pystray.MenuItem(
                "Quit",
                self._quit_app,
                enabled=True
            )
        }
        
        # Create the menu
        menu_items = list(self.menu_items.values())
        self.menu = pystray.Menu(*menu_items)
    
    def _update_menu_status(self) -> None:
        """
        Update the status text in the menu.
        """
        if self.icon:
            # Update status text
            if not self.orchestrator or not self.orchestrator.is_running:
                status_text = "Status: Stopped"
            elif self.is_paused:
                status_text = "Status: Paused"
            else:
                status_text = "Status: Running"
            
            # Update the menu item
            self.menu_items["status"].text = status_text
            
            # Update pause/resume button text
            if self.is_paused:
                self.menu_items["pause_resume"].text = "Resume Indexing"
            else:
                self.menu_items["pause_resume"].text = "Pause Indexing"
            
            # Refresh the menu
            self.icon.update_menu()
    
    def _toggle_pause(self) -> None:
        """
        Toggle the indexing pause state.
        """
        self.is_paused = not self.is_paused
        
        if self.orchestrator:
            if self.is_paused:
                # Stop the watcher when pausing
                if self.orchestrator.watcher:
                    self.orchestrator.watcher.stop()
                print("[Tray] Indexing paused")
            else:
                # Restart the watcher when resuming
                if self.orchestrator.watcher and self.orchestrator.is_running:
                    self.orchestrator.watcher.start()
                print("[Tray] Indexing resumed")
        
        self._update_menu_status()
        
        # Show notification
        self._show_notification(
            "File Indexer",
            f"Indexing {'paused' if self.is_paused else 'resumed'}"
        )
    
    def _open_search_app(self) -> None:
        """
        Launch the search application as a subprocess.
        """
        try:
            # Find the search script path
            project_root = Path(__file__).parent.parent
            search_script = project_root / "run_search.py"
            
            if not search_script.exists():
                self._show_notification(
                    "File Indexer",
                    "Search app not found!",
                    critical=True
                )
                return
            
            # Launch the search app
            if sys.platform == "win32":
                subprocess.Popen(
                    [sys.executable, str(search_script)],
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )
            else:
                subprocess.Popen(
                    [sys.executable, str(search_script)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            
            print("[Tray] Launched search app")
            
        except Exception as e:
            print(f"[Tray] Error launching search app: {e}")
            self._show_notification(
                "File Indexer",
                f"Error launching search app: {e}",
                critical=True
            )
    
    def _show_stats(self) -> None:
        """
        Show statistics in a popup notification.
        """
        if not self.orchestrator or not self.orchestrator.db:
            self._show_notification(
                "File Indexer",
                "Indexer not initialized"
            )
            return
        
        try:
            # Get database stats
            stats = self.orchestrator.db.get_all_stats()
            
            # Format the stats message
            message = f"Total Files: {stats['total_files']}\n"
            message += f"Document Types: {len(stats['by_document_type'])}\n"
            
            # Add top 3 document types
            if stats['by_document_type']:
                top_types = list(stats['by_document_type'].items())[:3]
                message += "\nTop Types:\n"
                for doc_type, count in top_types:
                    message += f"  {doc_type}: {count}\n"
            
            # Add last indexed time
            if stats['last_indexed']:
                try:
                    last_time = datetime.fromisoformat(stats['last_indexed'])
                    message += f"\nLast Indexed:\n{last_time.strftime('%Y-%m-%d %H:%M:%S')}"
                except:
                    message += f"\nLast Indexed: {stats['last_indexed']}"
            else:
                message += "\nLast Indexed: Never"
            
            # Add runtime stats
            if self.orchestrator.stats:
                message += f"\n\nSession Stats:\n"
                message += f"  Indexed: {self.orchestrator.stats['processed']}\n"
                message += f"  Failed: {self.orchestrator.stats['failed']}"
            
            self._show_notification("File Indexer Stats", message)
            
        except Exception as e:
            print(f"[Tray] Error getting stats: {e}")
            self._show_notification(
                "File Indexer",
                f"Error retrieving stats: {e}",
                critical=True
            )
    
    def _show_notification(self, title: str, message: str, critical: bool = False) -> None:
        """
        Show a system notification.
        
        Args:
            title: Notification title
            message: Notification message
            critical: Whether this is a critical notification
        """
        if not self.icon:
            return
        
        try:
            # Use pystray's notification system
            self.icon.notify(message, title)
        except Exception as e:
            # Fallback to print
            print(f"[Notification] {title}: {message}")
    
    def _run_orchestrator(self) -> None:
        """
        Run the orchestrator in a background thread.
        """
        try:
            # Initialize orchestrator
            self.orchestrator = FileIndexerOrchestrator(config_path=self.config_path)
            
            # Start the orchestrator
            self.orchestrator.start()
            
        except Exception as e:
            print(f"[Tray] Orchestrator error: {e}")
            self._show_notification(
                "File Indexer Error",
                f"Orchestrator failed: {e}",
                critical=True
            )
    
    def _update_status_periodically(self) -> None:
        """
        Periodically update the menu status.
        """
        def update_loop():
            while self.icon and self.icon._running:
                time.sleep(2)  # Update every 2 seconds
                if self.icon and self.icon._running:
                    self._update_menu_status()
        
        # Start in background thread
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()
    
    def _quit_app(self) -> None:
        """
        Quit the application cleanly.
        """
        print("[Tray] Shutting down...")
        
        # Stop orchestrator
        if self.orchestrator:
            self.orchestrator.stop()
        
        # Stop the status update thread
        if self.status_update_timer:
            self.status_update_timer.cancel()
        
        # Stop the icon
        if self.icon:
            self.icon.stop()
        
        print("[Tray] Goodbye!")
        sys.exit(0)
    
    def run(self) -> None:
        """
        Run the system tray application.
        """
        # Create the icon
        self.icon = pystray.Icon(
            "file_indexer",
            self.icon_image,
            "File Indexer",
            self.menu
        )
        
        # Start orchestrator in background thread
        orchestrator_thread = threading.Thread(target=self._run_orchestrator, daemon=True)
        orchestrator_thread.start()
        
        # Wait for orchestrator to initialize
        time.sleep(3)
        
        # Start periodic status updates
        self._update_status_periodically()
        
        # Update initial status
        self._update_menu_status()
        
        # Show startup notification
        self._show_notification(
            "File Indexer",
            "Indexer started and running in background"
        )
        
        # Run the icon (blocks until quit)
        print("[Tray] File Indexer running in system tray")
        print("[Tray] Right-click the icon to access menu")
        print("[Tray] Press Ctrl+C in terminal to quit")
        
        try:
            self.icon.run()
        except KeyboardInterrupt:
            self._quit_app()


def run_tray_app(config_path: Optional[str] = None) -> None:
    """
    Run the tray application.
    
    Args:
        config_path: Optional path to configuration file
    """
    app = TrayApp(config_path=config_path)
    app.run()


def main():
    """Main entry point for the tray app."""
    import argparse
    
    parser = argparse.ArgumentParser(description="File Indexer System Tray App")
    parser.add_argument("--config", "-c", help="Path to configuration file")
    
    args = parser.parse_args()
    
    run_tray_app(config_path=args.config)


if __name__ == "__main__":
    main()


