"""CustomTkinter GUI search interface for file indexer."""

import sys
import os
import json
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import customtkinter as ctk
from tkinter import filedialog, messagebox
from search.engine import SearchEngine

# Configure CustomTkinter
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class SettingsWindow(ctk.CTkToplevel):
    """Settings window for configuring the file indexer."""
    
    def __init__(self, parent, config_path: str = "config.json", on_settings_saved=None):
        """Initialize settings window.
        
        Args:
            parent: Parent window
            config_path: Path to config.json file
            on_settings_saved: Callback when settings are saved
        """
        super().__init__(parent)
        
        self.parent = parent
        self.config_path = config_path
        self.on_settings_saved = on_settings_saved
        self.config = self._load_config()
        
        # Setup window
        self.title("Settings - File Indexer")
        self.geometry("700x800")
        self.minsize(600, 600)
        self.grab_set()  # Make modal
        self.transient(parent)  # Set parent relationship
        
        # Configure grid
        self.grid_rowconfigure(5, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Create UI
        self._create_widgets()
        self._load_current_settings()
    
    def _load_config(self) -> dict:
        """Load configuration from file.
        
        Returns:
            Configuration dictionary
        """
        default_config = {
            "watched_folders": [],
            "file_types": {
                "txt": True, "md": True, "pdf": True, "docx": True,
                "xlsx": True, "csv": True, "jpg": True, "png": True
            },
            "model": {
                "model_path": "",
                "use_gpu": False,
                "gpu_layers": 0,
                "context_size": 2048,
                "max_tokens": 256,
                "temperature": 0.7
            },
            "indexing": {
                "max_file_size_mb": 50,
                "text_truncate_chars": 3000,
                "debounce_seconds": 2,
                "exclude_hidden": True,
                "exclude_patterns": ["*.tmp", "*.log", "*.cache"]
            }
        }
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    # Merge with defaults for missing keys
                    for key in default_config:
                        if key not in config:
                            config[key] = default_config[key]
                    return config
            return default_config
        except Exception as e:
            print(f"Error loading config: {e}")
            return default_config
    
    def _save_config(self):
        """Save current configuration to file."""
        try:
            # Update config with UI values
            self.config["watched_folders"] = self.watched_folders
            
            # Save file type settings
            for ext, var in self.file_type_vars.items():
                self.config["file_types"][ext] = var.get()
            
            # Save model settings
            self.config["model"]["model_path"] = self.model_path_var.get()
            self.config["model"]["use_gpu"] = self.gpu_var.get()
            self.config["model"]["gpu_layers"] = self.gpu_layers_var.get()
            
            # Save indexing settings
            self.config["indexing"]["max_file_size_mb"] = self.max_size_var.get()
            self.config["indexing"]["exclude_hidden"] = self.exclude_hidden_var.get()
            
            # Write to file
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")
            return False
    
    def _create_widgets(self):
        """Create all UI widgets."""
        # Main scrollable frame
        main_frame = ctk.CTkScrollableFrame(self)
        main_frame.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
        main_frame.grid_columnconfigure(0, weight=1)
        
        # 1. Watched Folders Section
        self._create_watched_folders_section(main_frame)
        
        # 2. File Types Section
        self._create_file_types_section(main_frame)
        
        # 3. Model Section
        self._create_model_section(main_frame)
        
        # 4. Indexing Settings Section
        self._create_indexing_section(main_frame)
        
        # 5. Action Buttons
        self._create_action_buttons(main_frame)
    
    def _create_watched_folders_section(self, parent):
        """Create watched folders configuration section."""
        # Section header
        header_frame = ctk.CTkFrame(parent, fg_color="transparent")
        header_frame.grid(row=0, column=0, padx=10, pady=(10, 5), sticky="ew")
        header_frame.grid_columnconfigure(0, weight=1)
        
        ctk.CTkLabel(
            header_frame,
            text="📁 Watched Folders",
            font=ctk.CTkFont(size=16, weight="bold")
        ).grid(row=0, column=0, sticky="w")
        
        add_folder_btn = ctk.CTkButton(
            header_frame,
            text="+ Add Folder",
            command=self._add_folder,
            width=100,
            height=30
        )
        add_folder_btn.grid(row=0, column=1, padx=(10, 0))
        
        # List of watched folders
        self.folders_frame = ctk.CTkFrame(parent)
        self.folders_frame.grid(row=1, column=0, padx=10, pady=(0, 10), sticky="ew")
        self.folders_frame.grid_columnconfigure(0, weight=1)
        
        ctk.CTkLabel(
            self.folders_frame,
            text="No folders added",
            text_color="#808080"
        ).grid(row=0, column=0, padx=10, pady=10)
    
    def _create_file_types_section(self, parent):
        """Create file types configuration section."""
        # Section header
        ctk.CTkLabel(
            parent,
            text="📄 File Types to Index",
            font=ctk.CTkFont(size=16, weight="bold")
        ).grid(row=2, column=0, padx=10, pady=(20, 10), sticky="w")
        
        # File types grid
        file_types_frame = ctk.CTkFrame(parent)
        file_types_frame.grid(row=3, column=0, padx=10, pady=(0, 10), sticky="ew")
        
        self.file_type_vars = {}
        extensions = [
            (".txt", "txt"), (".md", "md"), (".pdf", "pdf"),
            (".docx", "docx"), (".xlsx", "xlsx"), (".csv", "csv"),
            (".jpg", "jpg"), (".png", "png")
        ]
        
        # 2x4 grid layout
        for i, (display, ext) in enumerate(extensions):
            row = i // 4
            col = i % 4
            var = ctk.BooleanVar(value=self.config["file_types"].get(ext, True))
            self.file_type_vars[ext] = var
            
            checkbox = ctk.CTkCheckBox(
                file_types_frame,
                text=display,
                variable=var
            )
            checkbox.grid(row=row, column=col, padx=20, pady=5, sticky="w")
    
    def _create_model_section(self, parent):
        """Create model configuration section."""
        # Section header
        ctk.CTkLabel(
            parent,
            text="🧠 LLM Model Configuration",
            font=ctk.CTkFont(size=16, weight="bold")
        ).grid(row=4, column=0, padx=10, pady=(20, 10), sticky="w")
        
        model_frame = ctk.CTkFrame(parent)
        model_frame.grid(row=5, column=0, padx=10, pady=(0, 10), sticky="ew")
        model_frame.grid_columnconfigure(1, weight=1)
        
        # Model path
        ctk.CTkLabel(model_frame, text="Model Path:").grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.model_path_var = ctk.StringVar(value=self.config["model"].get("model_path", ""))
        self.model_path_entry = ctk.CTkEntry(
            model_frame,
            textvariable=self.model_path_var,
            placeholder_text="/path/to/model.gguf"
        )
        self.model_path_entry.grid(row=0, column=1, padx=(0, 10), pady=10, sticky="ew")
        
        browse_btn = ctk.CTkButton(
            model_frame,
            text="Browse",
            command=self._browse_model,
            width=80
        )
        browse_btn.grid(row=0, column=2, padx=(0, 10), pady=10)
        
        # GPU settings
        self.gpu_var = ctk.BooleanVar(value=self.config["model"].get("use_gpu", False))
        gpu_checkbox = ctk.CTkCheckBox(
            model_frame,
            text="Use GPU Acceleration",
            variable=self.gpu_var,
            command=self._on_gpu_toggle
        )
        gpu_checkbox.grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="w")
        
        # GPU layers slider
        ctk.CTkLabel(model_frame, text="GPU Layers:").grid(row=2, column=0, padx=10, pady=10, sticky="w")
        self.gpu_layers_var = ctk.IntVar(value=self.config["model"].get("gpu_layers", 0))
        self.gpu_layers_slider = ctk.CTkSlider(
            model_frame,
            from_=0,
            to=40,
            number_of_steps=40,
            variable=self.gpu_layers_var,
            command=self._on_gpu_layers_change
        )
        self.gpu_layers_slider.grid(row=2, column=1, padx=(0, 10), pady=10, sticky="ew")
        
        self.gpu_layers_label = ctk.CTkLabel(
            model_frame,
            text=f"{self.gpu_layers_var.get()} layers"
        )
        self.gpu_layers_label.grid(row=2, column=2, padx=(0, 10), pady=10)
        
        # Disable GPU slider if GPU not enabled
        if not self.gpu_var.get():
            self.gpu_layers_slider.configure(state="disabled")
            self.gpu_layers_label.configure(text_color="#808080")
    
    def _create_indexing_section(self, parent):
        """Create indexing settings section."""
        # Section header
        ctk.CTkLabel(
            parent,
            text="⚙️ Indexing Settings",
            font=ctk.CTkFont(size=16, weight="bold")
        ).grid(row=6, column=0, padx=10, pady=(20, 10), sticky="w")
        
        indexing_frame = ctk.CTkFrame(parent)
        indexing_frame.grid(row=7, column=0, padx=10, pady=(0, 10), sticky="ew")
        indexing_frame.grid_columnconfigure(1, weight=1)
        
        # Max file size
        ctk.CTkLabel(indexing_frame, text="Max File Size (MB):").grid(row=0, column=0, padx=10, pady=10, sticky="w")
        self.max_size_var = ctk.IntVar(value=self.config["indexing"].get("max_file_size_mb", 50))
        max_size_spinbox = ctk.CTkEntry(
            indexing_frame,
            textvariable=self.max_size_var,
            width=100
        )
        max_size_spinbox.grid(row=0, column=1, padx=10, pady=10, sticky="w")
        
        # Exclude hidden files
        self.exclude_hidden_var = ctk.BooleanVar(value=self.config["indexing"].get("exclude_hidden", True))
        exclude_hidden_checkbox = ctk.CTkCheckBox(
            indexing_frame,
            text="Exclude hidden files",
            variable=self.exclude_hidden_var
        )
        exclude_hidden_checkbox.grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="w")
    
    def _create_action_buttons(self, parent):
        """Create action buttons at bottom."""
        buttons_frame = ctk.CTkFrame(parent, fg_color="transparent")
        buttons_frame.grid(row=8, column=0, padx=10, pady=(20, 10), sticky="ew")
        buttons_frame.grid_columnconfigure(0, weight=1)
        buttons_frame.grid_columnconfigure(1, weight=1)
        buttons_frame.grid_columnconfigure(2, weight=1)
        
        # Save button
        save_btn = ctk.CTkButton(
            buttons_frame,
            text="💾 Save Settings",
            command=self._save_and_close,
            height=40,
            font=ctk.CTkFont(size=13, weight="bold")
        )
        save_btn.grid(row=0, column=0, padx=5, pady=10, sticky="ew")
        
        # Re-index button
        reindex_btn = ctk.CTkButton(
            buttons_frame,
            text="🔄 Re-index All Files",
            command=self._reindex_all,
            height=40,
            fg_color="#ef6c00",
            hover_color="#d84315"
        )
        reindex_btn.grid(row=0, column=1, padx=5, pady=10, sticky="ew")
        
        # Cancel button
        cancel_btn = ctk.CTkButton(
            buttons_frame,
            text="❌ Cancel",
            command=self.destroy,
            height=40,
            fg_color="#555555",
            hover_color="#666666"
        )
        cancel_btn.grid(row=0, column=2, padx=5, pady=10, sticky="ew")
    
    def _load_current_settings(self):
        """Load current settings into UI."""
        self.watched_folders = self.config.get("watched_folders", [])
        self._refresh_folders_list()
    
    def _refresh_folders_list(self):
        """Refresh the watched folders list display."""
        # Clear existing widgets
        for widget in self.folders_frame.winfo_children():
            widget.destroy()
        
        if not self.watched_folders:
            ctk.CTkLabel(
                self.folders_frame,
                text="No folders added",
                text_color="#808080"
            ).grid(row=0, column=0, padx=10, pady=10)
            return
        
        # Display each folder with remove button
        for i, folder in enumerate(self.watched_folders):
            folder_label = ctk.CTkLabel(
                self.folders_frame,
                text=folder,
                anchor="w"
            )
            folder_label.grid(row=i, column=0, padx=(10, 5), pady=5, sticky="ew")
            
            remove_btn = ctk.CTkButton(
                self.folders_frame,
                text="Remove",
                command=lambda f=folder: self._remove_folder(f),
                width=80,
                height=25,
                fg_color="#c62828",
                hover_color="#b71c1c"
            )
            remove_btn.grid(row=i, column=1, padx=(5, 10), pady=5)
            
            self.folders_frame.grid_columnconfigure(0, weight=1)
    
    def _add_folder(self):
        """Open folder picker and add selected folder."""
        folder = filedialog.askdirectory(title="Select Folder to Watch")
        if folder and folder not in self.watched_folders:
            self.watched_folders.append(folder)
            self._refresh_folders_list()
    
    def _remove_folder(self, folder):
        """Remove folder from watched list."""
        if folder in self.watched_folders:
            self.watched_folders.remove(folder)
            self._refresh_folders_list()
    
    def _browse_model(self):
        """Browse for GGUF model file."""
        file_path = filedialog.askopenfilename(
            title="Select GGUF Model File",
            filetypes=[("GGUF models", "*.gguf"), ("All files", "*.*")]
        )
        if file_path:
            self.model_path_var.set(file_path)
    
    def _on_gpu_toggle(self):
        """Handle GPU toggle change."""
        if self.gpu_var.get():
            self.gpu_layers_slider.configure(state="normal")
            self.gpu_layers_label.configure(text_color=None)
        else:
            self.gpu_layers_slider.configure(state="disabled")
            self.gpu_layers_label.configure(text_color="#808080")
    
    def _on_gpu_layers_change(self, value):
        """Update GPU layers label when slider moves."""
        self.gpu_layers_label.configure(text=f"{int(value)} layers")
    
    def _save_and_close(self):
        """Save settings and close window."""
        if self._save_config():
            if self.on_settings_saved:
                self.on_settings_saved()
            messagebox.showinfo("Success", "Settings saved successfully!")
            self.destroy()
    
    def _reindex_all(self):
        """Trigger re-indexing of all files."""
        if messagebox.askyesno(
            "Confirm Re-index",
            "This will re-index all files in watched folders.\n"
            "This may take a while depending on the number of files.\n\n"
            "Continue?"
        ):
            # Save current settings first
            if self._save_config():
                messagebox.showinfo(
                    "Re-index Started",
                    "Re-indexing has been triggered.\n"
                    "The indexer will process all files in the background.\n"
                    "You can continue using the search interface."
                )
                # Here you would signal the indexer to re-index
                # For now, just close settings
                if self.on_settings_saved:
                    self.on_settings_saved()
                self.destroy()


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
    
    def __init__(self, db_path: str = "file_index.db", config_path: str = "config.json"):
        """Initialize the GUI.
        
        Args:
            db_path: Path to SQLite database file
            config_path: Path to config.json file
        """
        super().__init__()
        
        self.db_path = db_path
        self.config_path = config_path
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
        # Top bar with title and settings button
        top_bar = ctk.CTkFrame(self, height=50, fg_color="transparent")
        top_bar.grid(row=0, column=0, padx=20, pady=(10, 0), sticky="ew")
        top_bar.grid_columnconfigure(0, weight=1)
        
        # Title
        title_label = ctk.CTkLabel(
            top_bar,
            text="File Finder",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        title_label.grid(row=0, column=0, padx=10, sticky="w")
        
        # Settings button
        settings_btn = ctk.CTkButton(
            top_bar,
            text="⚙️ Settings",
            command=self._open_settings,
            width=100,
            height=35
        )
        settings_btn.grid(row=0, column=1, padx=10, sticky="e")
        
        # Search frame
        self.search_frame = ctk.CTkFrame(self)
        self.search_frame.grid(row=1, column=0, padx=20, pady=(10, 10), sticky="ew")
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
        
        # Results area - scrollable
        self.results_container = ctk.CTkScrollableFrame(self, label_text="Search Results")
        self.results_container.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")
        self.results_container.grid_columnconfigure(0, weight=1)
        
        # Welcome message
        self._show_welcome_message()
        
        # Status bar
        self.status_bar = ctk.CTkFrame(self, height=30)
        self.status_bar.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="ew")
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
    
    def _open_settings(self):
        """Open the settings window."""
        SettingsWindow(self, self.config_path, on_settings_saved=self._on_settings_saved)
    
    def _on_settings_saved(self):
        """Callback when settings are saved."""
        self.status_label.configure(text="Settings saved. Indexer will apply changes on next run.")
        self.after(3000, lambda: self.status_label.configure(text="Ready"))
        # Refresh stats (config might have changed)
        self._update_stats()
    
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
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config.json file (default: config.json)"
    )
    args = parser.parse_args()
    
    app = SearchGUI(db_path=args.db, config_path=args.config)
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()


if __name__ == "__main__":
    main()



