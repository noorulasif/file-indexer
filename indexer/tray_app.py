"""
System tray application for the File Indexer.
Provides a GUI interface to control the indexing service.

Linux note: requires a system tray host. Install one of:
  - GNOME: gnome-shell-extension-appindicator
  - KDE:   built-in
  - Other: snixembed or stalonetray

Install Python deps:
  pip install pystray Pillow
"""

import sys
import threading
import subprocess
import time
import logging
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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Icon factory — shared by both entry points
# ---------------------------------------------------------------------------

def _create_icon_image() -> Image.Image:
    """
    Create a programmatic 64x64 tray icon: blue square with a white 'F'.
    No external image file needed.
    """
    size = 64
    image = Image.new("RGB", (size, size), color=(33, 150, 243))  # Material blue
    draw = ImageDraw.Draw(image)

    # Try bold system fonts; fall back to PIL default
    font = None
    font_candidates = [
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",  # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",          # Linux alt
        "arialbd.ttf",                                                    # Windows bold
        "arial.ttf",                                                      # Windows
    ]
    for path in font_candidates:
        try:
            font = ImageFont.truetype(path, 40)
            break
        except (IOError, OSError):
            continue
    if font is None:
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), "F", font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    x = (size - text_w) // 2
    y = (size - text_h) // 2 - 2

    draw.text((x, y), "F", fill=(255, 255, 255), font=font)
    return image


# ---------------------------------------------------------------------------
# TrayApp
# ---------------------------------------------------------------------------

class TrayApp:
    """
    System tray application that manages the File Indexer.

    Accepts a pre-initialised FileIndexerOrchestrator so that
    run_indexer.py controls the lifecycle; the tray just drives it.
    """

    def __init__(self, orchestrator: FileIndexerOrchestrator):
        self.orchestrator = orchestrator
        self.is_paused = False
        self.icon: Optional[pystray.Icon] = None

        # Event used to stop the status-polling thread cleanly
        self._stop_event = threading.Event()

        # Give the orchestrator a back-channel to show notifications
        orchestrator.show_notification = self._show_notification

        self.icon_image = _create_icon_image()
        self.menu = self._build_menu()

    # ------------------------------------------------------------------
    # Menu construction
    # ------------------------------------------------------------------

    def _build_menu(self) -> pystray.Menu:
        """Build the right-click menu with dynamic items."""

        # Dynamic items need to be callables so pystray re-evaluates them
        # each time the menu is shown.

        def status_text(item):
            if not self.orchestrator.is_running:
                return "Status: Stopped"
            return "Status: Paused" if self.is_paused else "Status: Running"

        def pause_resume_text(item):
            return "Resume Indexing" if self.is_paused else "Pause Indexing"

        return pystray.Menu(
            pystray.MenuItem("File Indexer", None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(status_text, None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(pause_resume_text, self._toggle_pause),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open Search App", self._open_search_app),
            pystray.MenuItem("View Stats",      self._show_stats),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit",            self._quit_app),
        )

    # ------------------------------------------------------------------
    # Menu callbacks
    # ------------------------------------------------------------------

    def _toggle_pause(self) -> None:
        """
        Pause or resume indexing.

        Rather than stopping/restarting the watchdog observer (which
        watchdog does not support), we flip a flag on the orchestrator.
        The orchestrator's _watcher_callback already checks is_paused
        and skips processing when True.
        """
        self.is_paused = not self.is_paused
        self.orchestrator.is_paused = self.is_paused

        state = "paused" if self.is_paused else "resumed"
        logger.info(f"[Tray] Indexing {state}")
        self._show_notification("File Indexer", f"Indexing {state}")

        if self.icon:
            self.icon.update_menu()

    def _open_search_app(self) -> None:
        """Launch run_search.py as a detached subprocess."""
        try:
            search_script = Path(__file__).parent.parent / "run_search.py"

            if not search_script.exists():
                self._show_notification("File Indexer", "Search app not found!")
                logger.warning(f"[Tray] run_search.py not found at {search_script}")
                return

            if sys.platform == "win32":
                subprocess.Popen(
                    [sys.executable, str(search_script)],
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                )
            else:
                subprocess.Popen(
                    [sys.executable, str(search_script)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

            logger.info("[Tray] Launched search app")

        except Exception as e:
            logger.error(f"[Tray] Error launching search app: {e}")
            self._show_notification("File Indexer", f"Could not open search app: {e}")

    def _show_stats(self) -> None:
        """Show a stats summary via system notification."""
        try:
            db_stats = self.orchestrator.db.get_all_stats()

            last = db_stats.get("last_indexed") or "Never"
            if last != "Never":
                try:
                    last = datetime.fromisoformat(last).strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass

            lines = [
                f"Total files:  {db_stats['total_files']}",
                f"Last indexed: {last}",
            ]

            # Top 3 document types
            by_type = db_stats.get("by_document_type", {})
            if by_type:
                lines.append("Top types:")
                for doc_type, count in list(by_type.items())[:3]:
                    lines.append(f"  {doc_type}: {count}")

            # Session stats
            sess = self.orchestrator.stats
            if sess.get("processed", 0) > 0:
                lines.append(f"Session: {sess['processed']} indexed, {sess['failed']} failed")

            self._show_notification("File Indexer Stats", "\n".join(lines))

        except Exception as e:
            logger.error(f"[Tray] Error getting stats: {e}")
            self._show_notification("File Indexer", f"Could not retrieve stats: {e}")

    def _show_notification(self, title: str, message: str, **kwargs) -> None:
        """Show a system notification via the tray icon (fallback: log)."""
        if self.icon:
            try:
                self.icon.notify(message, title)
                return
            except Exception:
                pass
        # Fallback when icon isn't ready yet or notify isn't supported
        logger.info(f"[Notification] {title}: {message}")

    def _quit_app(self) -> None:
        """Stop the orchestrator and exit cleanly."""
        logger.info("[Tray] Shutting down...")
        self._stop_event.set()          # Stop the status-poll thread

        if self.orchestrator:
            self.orchestrator.stop()

        if self.icon:
            self.icon.stop()            # Let icon.run() return naturally
        # Do NOT call sys.exit() here — it prevents icon cleanup on Linux

    # ------------------------------------------------------------------
    # Background status polling
    # ------------------------------------------------------------------

    def _status_poll_loop(self) -> None:
        """
        Runs in a daemon thread.
        Calls icon.update_menu() every 5 seconds so the dynamic status
        text stays current without any private pystray internals.
        """
        while not self._stop_event.wait(timeout=5):
            if self.icon:
                try:
                    self.icon.update_menu()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Start the orchestrator (if not already running), create the tray
        icon, and block until the user clicks Quit.
        """
        # Start the orchestrator in a background thread if needed
        if not self.orchestrator.is_running:
            t = threading.Thread(target=self.orchestrator.start, daemon=True)
            t.start()

        # Start the status-polling thread
        poll_thread = threading.Thread(target=self._status_poll_loop, daemon=True)
        poll_thread.start()

        # Create and run the icon (blocks until icon.stop() is called)
        self.icon = pystray.Icon(
            "file_indexer",
            self.icon_image,
            "File Indexer",
            self.menu,
        )

        logger.info("[Tray] File Indexer running in system tray")
        logger.info("[Tray] Right-click the tray icon to access controls")

        try:
            self.icon.run()
        except KeyboardInterrupt:
            self._quit_app()


# ---------------------------------------------------------------------------
# Public entry points called by run_indexer.py
# ---------------------------------------------------------------------------

def run_tray_app_with_orchestrator(orchestrator: FileIndexerOrchestrator) -> None:
    """
    Run the tray application with a pre-initialised orchestrator.
    Called by run_indexer.py when --no-tray is NOT set.
    """
    app = TrayApp(orchestrator)
    app.run()


def run_tray_app(config_path: Optional[str] = None) -> None:
    """
    Convenience entry point: create an orchestrator internally and run.
    Useful when launching tray_app.py directly.
    """
    orchestrator = FileIndexerOrchestrator(config_path=config_path)
    app = TrayApp(orchestrator)
    app.run()


# ---------------------------------------------------------------------------
# Direct invocation
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="File Indexer System Tray")
    parser.add_argument("--config", "-c", help="Path to configuration file")
    args = parser.parse_args()
    run_tray_app(config_path=args.config)


if __name__ == "__main__":
    main()



