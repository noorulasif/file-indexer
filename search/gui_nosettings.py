"""CustomTkinter GUI search interface for file indexer."""

import sys
import os
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import customtkinter as ctk
from search.engine import SearchEngine

# Configure CustomTkinter
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class FileResultCard(ctk.CTkFrame):
    """Card widget for displaying a single search result."""
    
    def __init__(self, parent, result: Dict[str, Any], engine: SearchEngine, 
                 on_file_opened=None):
        """Initialize result card.
        
        Args:
            parent: Parent widget
            result: Search result dictionary
            engine: SearchEngine instance
            on_file_opened: Callback when file is opened
        """
        super().__init__(parent, corner_radius=10, border_width=1)
        self.result = result
        self.engine = engine
        self.on_file_opened = on_file_opened
        
        # Set border color based on file existence
        if result.get('exists', False):
            self.configure(border_color="#2b2b2b")
        else:
            self.configure(border_color="#5a2a2a")
        
        self._create_widgets()
        self._layout_widgets()
    
    def _create_widgets(self):
        """Create all widgets for the card."""
        # File name and status
        file_name = self.result.get('file_name', 'Unknown')
        if not self.result.get('exists', False):
            file_name = f"🚫 {file_name} (MISSING)"
        
        self.name_label = ctk.CTkLabel(
            self,
            text=file_name,
            font=ctk.CTkFont(size=16, weight="bold"),
            anchor="w"
        )
        
        # Document type badge
        doc_type = self.result.get('document_type', 'other')
        badge_colors = {
            'invoice': "#2e7d32",
            'resume': "#1565c0",
            'passport': "#6a1b9a",
            'certificate': "#c62828",
            'photo': "#ef6c00",
            'report': "#4527a0",
            'contract': "#00838f",
            'notes': "#2c3e50",
            'spreadsheet': "#2e7d32",
            'code': "#1a237e",
            'personal': "#ad1457",
            'other': "#546e7f"
        }
        badge_color = badge_colors.get(doc_type, "#546e7f")
        
        self.type_badge = ctk.CTkLabel(
            self,
            text=f"  {doc_type.upper()}  ",
            font=ctk.CTkFont(size=11, weight="bold"),
            fg_color=badge_color,
            corner_radius=5,
            padx=5
        )
        
        # Summary text
        summary = self.result.get('summary', 'No summary available')
        self.summary_label = ctk.CTkLabel(
            self,
            text=summary,
            font=ctk.CTkFont(size=12),
            anchor="w",
            wraplength=650
        )
        
        # Tags as chips
        self.tags_frame = ctk.CTkFrame(self, fg_color="transparent")
        tags = self.result.get('tags', [])
        self.tag_chips = []
        
        for tag in tags[:5]:  # Limit to 5 tags
            chip = ctk.CTkLabel(
                self.tags_frame,
                text=f"  #{tag}  ",
                font=ctk.CTkFont(size=10),
                fg_color="#3a3a3a",
                corner_radius=8,
                padx=4,
                pady=2
            )
            self.tag_chips.append(chip)
        
        # File path
        file_path = self.result.get('file_path', '')
        self.path_label = ctk.CTkLabel(
            self,
            text=file_path,
            font=ctk.CTkFont(size=10),
            text_color="#808080",
            anchor="w"
        )
        
        # Action buttons
        self.open_file_btn = ctk.CTkButton(
            self,
            text="📄 Open File",
            command=self._open_file,
            height=30,
            width=100
        )
        
        self.open_folder_btn = ctk.CTkButton(
            self,
            text="📁 Open Folder",
            command=self._open_folder,
            height=30,
            width=100,
            fg_color="#3a3a3a",
            hover_color="#4a4a4a"
        )
        
        # Disable buttons if file missing
        if not self.result.get('exists', False):
            self.open_file_btn.configure(state="disabled", fg_color="#4a4a4a")
            self.open_folder_btn.configure(state="disabled", fg_color="#4a4a4a")
    
    def _layout_widgets(self):
        """Layout all widgets in the card."""
        # Top row: name and type badge
        self.name_label.grid(row=0, column=0, padx=(15, 10), pady=(15, 5), sticky="w")
        self.type_badge.grid(row=0, column=1, padx=(0, 15), pady=(15, 5), sticky="e")
        
        # Summary
        self.summary_label.grid(row=1, column=0, columnspan=2, padx=15, pady=(0, 8), sticky="w")
        
        # Tags
        if self.tag_chips:
            self.tags_frame.grid(row=2, column=0, columnspan=2, padx=15, pady=(0, 8), sticky="w")
            for i, chip in enumerate(self.tag_chips):
                chip.pack(side="left", padx=(0, 5))
        
        # File path
        self.path_label.grid(row=3, column=0, columnspan=2, padx=15, pady=(0, 10), sticky="w")
        
        # Buttons
        self.open_file_btn.grid(row=4, column=0, padx=(15, 10), pady=(0, 15), sticky="w")
        self.open_folder_btn.grid(row=4, column=1, padx=(0, 15), pady=(0, 15), sticky="e")
        
        # Configure grid weights
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
    
    def _open_file(self):
        """Open the file."""
        if self.engine.open_file(self.result['file_path']):
            if self.on_file_opened:
                self.on_file_opened(self.result['file_name'])
    
    def _open_folder(self):
        """Open the folder containing the file."""
        self.engine.open_folder(self.result['file_path'])


class SearchGUI(ctk.CTk):
    """Main search GUI application."""
    
    def __init__(self, db_path: str = "file_index.db"):
        """Initialize the GUI.
        
        Args:
            db_path: Path to SQLite database file
        """
        super().__init__()
        
        self.db_path = db_path
        self.engine = SearchEngine(db_path)
        self.results = []
        
        # Setup window
        self.title("File Finder")
        self.geometry("800x600")
        self.minsize(600, 400)
        
        # Configure grid
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Create UI elements
        self._create_widgets()
        
        # Update stats on startup
        self.after(100, self._update_stats)
    
    def _create_widgets(self):
        """Create all UI widgets."""
        # Search frame (top)
        self.search_frame = ctk.CTkFrame(self)
        self.search_frame.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="ew")
        self.search_frame.grid_columnconfigure(0, weight=1)
        
        self.search_entry = ctk.CTkEntry(
            self.search_frame,
            placeholder_text="Search your files... (e.g., 'invoice 2024', 'passport', 'meeting notes')",
            font=ctk.CTkFont(size=13),
            height=40
        )
        self.search_entry.grid(row=0, column=0, padx=(10, 10), pady=10, sticky="ew")
        self.search_entry.bind("<Return>", lambda e: self._perform_search())
        
        self.search_btn = ctk.CTkButton(
            self.search_frame,
            text="🔍 Search",
            command=self._perform_search,
            height=40,
            width=100
        )
        self.search_btn.grid(row=0, column=1, padx=(0, 10), pady=10)
        
        # Results area (middle) - scrollable
        self.results_container = ctk.CTkScrollableFrame(self, label_text="Search Results")
        self.results_container.grid(row=1, column=0, padx=20, pady=10, sticky="nsew")
        self.results_container.grid_columnconfigure(0, weight=1)
        
        # Welcome message
        self._show_welcome_message()
        
        # Status bar (bottom)
        self.status_bar = ctk.CTkFrame(self, height=30)
        self.status_bar.grid(row=2, column=0, padx=20, pady=(0, 20), sticky="ew")
        self.status_bar.grid_columnconfigure(0, weight=1)
        
        self.status_label = ctk.CTkLabel(
            self.status_bar,
            text="Ready",
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        self.status_label.grid(row=0, column=0, padx=10, pady=5, sticky="w")
        
        self.stats_label = ctk.CTkLabel(
            self.status_bar,
            text="",
            font=ctk.CTkFont(size=11),
            anchor="e"
        )
        self.stats_label.grid(row=0, column=1, padx=10, pady=5, sticky="e")
    
    def _show_welcome_message(self):
        """Show welcome message when no search has been performed."""
        # Clear existing widgets
        for widget in self.results_container.winfo_children():
            widget.destroy()
        
        # Welcome frame
        welcome_frame = ctk.CTkFrame(self.results_container, fg_color="transparent")
        welcome_frame.grid(row=0, column=0, pady=50)
        
        welcome_text = ctk.CTkLabel(
            welcome_frame,
            text="🔍 Welcome to File Finder!",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        welcome_text.pack(pady=10)
        
        instruction_text = ctk.CTkLabel(
            welcome_frame,
            text="Enter a search query above to find your files\n\n"
                 "Try searching for:\n"
                 "• Document types: 'invoice', 'resume', 'report'\n"
                 "• Specific terms: 'passport', 'contract 2024'\n"
                 "• People: 'John Smith', 'Marketing team'\n\n"
                 "Search uses full-text indexing of file summaries and metadata",
            font=ctk.CTkFont(size=12),
            justify="center"
        )
        instruction_text.pack(pady=10)
    
    def _perform_search(self):
        """Perform search and update results display."""
        query = self.search_entry.get().strip()
        
        if not query:
            self._show_welcome_message()
            self.status_label.configure(text="Enter a search query")
            return
        
        # Update status
        self.status_label.configure(text=f"Searching for '{query}'...")
        self.update_idletasks()
        
        # Perform search (blocking but fast)
        try:
            self.results = self.engine.search(query, limit=50)
            self._display_results(query)
        except Exception as e:
            self._show_error(f"Search failed: {str(e)}")
        finally:
            self._update_stats()
    
    def _display_results(self, query: str):
        """Display search results as cards."""
        # Clear existing widgets
        for widget in self.results_container.winfo_children():
            widget.destroy()
        
        if not self.results:
            # No results message
            no_results_frame = ctk.CTkFrame(self.results_container, fg_color="transparent")
            no_results_frame.grid(row=0, column=0, pady=50)
            
            error_label = ctk.CTkLabel(
                no_results_frame,
                text=f"🔍 No results found for '{query}'",
                font=ctk.CTkFont(size=18)
            )
            error_label.pack(pady=10)
            
            suggestion_label = ctk.CTkLabel(
                no_results_frame,
                text="Try different keywords or check your indexer is running",
                font=ctk.CTkFont(size=12),
                text_color="#808080"
            )
            suggestion_label.pack()
            
            self.status_label.configure(text=f"No results found for '{query}'")
            return
        
        # Display each result as a card
        for i, result in enumerate(self.results):
            card = FileResultCard(
                self.results_container,
                result,
                self.engine,
                on_file_opened=self._on_file_opened
            )
            card.grid(row=i, column=0, padx=10, pady=(0, 10), sticky="ew")
        
        # Update status
        existing_count = sum(1 for r in self.results if r.get('exists', False))
        missing_count = len(self.results) - existing_count
        
        status_text = f"Found {len(self.results)} results for '{query}'"
        if missing_count > 0:
            status_text += f" ({missing_count} files missing from disk)"
        self.status_label.configure(text=status_text)
    
    def _update_stats(self):
        """Update statistics in status bar."""
        try:
            stats = self.engine.get_stats()
            total_files = stats.get('total_files', 0)
            last_indexed = stats.get('last_indexed', 'Never')
            
            if last_indexed != 'Never' and last_indexed:
                try:
                    # Format datetime
                    if isinstance(last_indexed, str):
                        dt = datetime.fromisoformat(last_indexed.replace(' ', 'T'))
                        last_indexed = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            
            self.stats_label.configure(
                text=f"📊 {total_files} files indexed | Last indexed: {last_indexed}"
            )
        except Exception as e:
            self.stats_label.configure(text="Stats unavailable")
    
    def _on_file_opened(self, file_name: str):
        """Callback when a file is opened successfully."""
        self.status_label.configure(text=f"Opened: {file_name}", text_color="#4caf50")
        self.after(3000, lambda: self.status_label.configure(text_color=None))
        self.after(3000, lambda: self._update_stats())
    
    def _show_error(self, message: str):
        """Show error message in status bar."""
        self.status_label.configure(text=f"Error: {message}", text_color="#ef5350")
        self.after(5000, lambda: self.status_label.configure(text_color=None))
    
    def on_closing(self):
        """Handle window close event."""
        self.engine.close()
        self.destroy()


def main():
    """Entry point for GUI application."""
    import argparse
    
    parser = argparse.ArgumentParser(description="File Indexer Search GUI")
    parser.add_argument(
        "--db",
        default="file_index.db",
        help="Path to SQLite database file (default: file_index.db)"
    )
    args = parser.parse_args()
    
    app = SearchGUI(db_path=args.db)
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()


if __name__ == "__main__":
    main()



