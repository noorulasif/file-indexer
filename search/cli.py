"""Terminal-based search interface for file indexer."""

import sys
import os
import argparse
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel
from rich import box
from rich.text import Text
from search.engine import SearchEngine


class SearchCLI:
    """Command-line interface for file search."""
    
    def __init__(self, db_path: str = "file_index.db"):
        """Initialize CLI with search engine.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.console = Console()
        self.engine = SearchEngine(db_path)
    
    def run(self, query: Optional[str] = None):
        """Run the CLI search interface.
        
        Args:
            query: Optional search query from command line
        """
        try:
            self.console.print("\n[bold cyan]🔍 File Indexer Search[/bold cyan]")
            while True:
                # Get search query
                if query:
                    search_term = query
                    query = None  # Only use once
                else:
                    self.console.print("[dim]Press Ctrl+C or type 'quit' to exit[/dim]\n")
                    search_term = Prompt.ask("Enter search query")
                
                # Check for exit
                if search_term.lower() in ['quit', 'exit', 'q']:
                    break
                
                # Perform search
                if search_term.strip():
                    self._display_results(search_term)
                else:
                    self.console.print("[yellow]Please enter a search term[/yellow]\n")
                
        except KeyboardInterrupt:
            self.console.print("\n[dim]Goodbye![/dim]")
        finally:
            self.engine.close()
    
    def show_stats(self):
        """Display database statistics."""
        stats = self.engine.get_stats()
        
        # Create statistics panel
        stats_text = f"""
[bold]📊 Indexer Statistics[/bold]

[cyan]Total files indexed:[/cyan] {stats['total_files']:,}
[cyan]Missing files:[/cyan] {stats['missing_files']:,}
[cyan]Files existing:[/cyan] {stats['total_files'] - stats['missing_files']:,}

[cyan]Last indexed:[/cyan] {stats['last_indexed'] or 'Never'}

[bold green]📁 Top File Types:[/bold green]
"""
        # Show top 10 file types
        sorted_types = sorted(stats['by_type'].items(), key=lambda x: x[1], reverse=True)[:10]
        for ext, count in sorted_types:
            stats_text += f"  • {ext}: {count:,} files\n"
        
        self.console.print(Panel(stats_text.rstrip(), title="Statistics", border_style="cyan"))
    
    def _display_results(self, query: str):
        """Search and display results in a formatted table.
        
        Args:
            query: Search query string
        """
        with self.console.status(f"[bold green]Searching for '{query}'..."):
            results = self.engine.search(query, limit=50)
        
        if not results:
            self.console.print(f"\n[yellow]No results found for '{query}'[/yellow]\n")
            return
        
        # Create results table
        table = Table(title=f"Search Results: '{query}'", box=box.ROUNDED)
        table.add_column("#", style="dim", width=4)
        table.add_column("File Name", style="bold", width=40)
        table.add_column("Type", width=10)
        table.add_column("Summary", width=50)
        table.add_column("Date", width=12)
        table.add_column("Path", width=40)
        
        for idx, result in enumerate(results, 1):
            # Determine color based on file existence
            if result['exists']:
                name_style = "green"
                status_icon = "✓"
            else:
                name_style = "red"
                status_icon = "✗"
            
            # Truncate summary to 60 chars
            summary = result.get('summary', 'No summary')[:57] + "..." if len(result.get('summary', '')) > 60 else result.get('summary', 'No summary')
            
            # Format date
            date_hint = result.get('date_hint') or result.get('indexed_at', '')[:10] or 'Unknown'
            
            # Get file type (extension)
            file_type = result.get('extension', 'unknown').upper()
            
            # Get path (truncate for display)
            file_path = result.get('file_path', '')
            path_display = file_path if len(file_path) <= 40 else "..." + file_path[-37:]
            
            # Add row with color coding
            table.add_row(
                f"{idx}",
                f"[{name_style}]{status_icon} {result['file_name']}[/{name_style}]",
                file_type,
                summary,
                str(date_hint),
                f"[dim]{path_display}[/dim]"
            )
        
        self.console.print(table)
        
        # Add note about missing files if any
        missing_count = sum(1 for r in results if not r['exists'])
        if missing_count > 0:
            self.console.print(f"\n[yellow]⚠ {missing_count} file(s) marked in red are missing from disk[/yellow]")
        
        # Ask about opening a file
        self._prompt_open_file(results)

    def _prompt_open_file(self, results: List[dict]):
        """Prompt user to open a file from results.
        
        Args:
            results: List of search results
        """
        self.console.print("\n[dim]Enter a number to open a file, or press Enter to continue[/dim]")
        choice = Prompt.ask("Open file", default="")
        
        if not choice:
            return
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(results):
                selected = results[idx]
                if selected['exists']:
                    self.console.print(f"[cyan]Opening: {selected['file_name']}[/cyan]")
                    if self.engine.open_file(selected['file_path']):
                        self.console.print("[green]✓ File opened successfully[/green]")
                    else:
                        self.console.print("[red]✗ Failed to open file[/red]")
                else:
                    self.console.print(f"[red]Cannot open missing file: {selected['file_name']}[/red]")
            else:
                self.console.print(f"[red]Invalid selection: {choice}[/red]")
        except ValueError:
            self.console.print(f"[red]Invalid number: {choice}[/red]")

def main():
    """Entry point for CLI search."""
    parser = argparse.ArgumentParser(
        description="File Indexer Search - Terminal interface for finding indexed files"
    )
    parser.add_argument(
        "query",
        nargs="?",
        help="Search query (if not provided, interactive mode is used)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics instead of searching"
    )
    parser.add_argument(
        "--db",
        default="file_index.db",
        help="Path to SQLite database file (default: file_index.db)"
    )
    
    args = parser.parse_args()
    
    # Create CLI instance
    cli = SearchCLI(db_path=args.db)
    
    # Show stats if requested
    if args.stats:
        cli.show_stats()
    elif args.query:
        # Single search mode - one query then exit
        cli._display_results(args.query)
        cli.engine.close()
    else:
        # Interactive mode
        cli.run()


if __name__ == "__main__":
    main()


