#!/usr/bin/env python3
"""
Main entry point for the File Indexer background service.
Starts the indexer with optional system tray integration and maintenance commands.
"""

import sys
import time
import signal
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path if needed
sys.path.insert(0, str(Path(__file__).parent))

from indexer.orchestrator import FileIndexerOrchestrator

# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def signal_handler(signum, frame):
    """
    Handle shutdown signals gracefully.
    """
    logger.info(f"\nReceived signal {signum}, shutting down...")
    if hasattr(signal_handler, 'orchestrator'):
        signal_handler.orchestrator.stop()
    sys.exit(0)


def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="File Indexer - Background indexing service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start indexer with system tray (default)
  python run_indexer.py
  
  # Start indexer without system tray (headless/server)
  python run_indexer.py --no-tray
  
  # Run maintenance commands
  python run_indexer.py --cleanup          # Remove missing files from database
  python run_indexer.py --reindex          # Full reindex of all files
  python run_indexer.py --stats            # Show detailed statistics
  python run_indexer.py --reindex-file /path/to/file.pdf  # Reindex single file
  
  # Run with custom config file
  python run_indexer.py --config /path/to/custom_config.json
  
  # Run once (no watching, just scan and exit)
  python run_indexer.py --once
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration file (default: config.json in project root)'
    )
    
    parser.add_argument(
        '--no-tray',
        action='store_true',
        help='Run without system tray (for headless/server environments)'
    )
    
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once (scan existing files and exit, no file watching)'
    )
    
    # Maintenance commands
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Remove database records for missing files and exit'
    )
    
    parser.add_argument(
        '--reindex',
        action='store_true',
        help='Clear database and reindex all files from scratch'
    )
    
    parser.add_argument(
        '--reindex-file',
        type=str,
        metavar='FILE_PATH',
        help='Force reindex a single file (ignores hash check)'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show detailed database statistics and exit'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    return parser.parse_args()


def print_banner():
    """Print the application banner."""
    print("=" * 60)
    print("FILE INDEXER - Intelligent File Management")
    print("=" * 60)
    print()


def show_detailed_stats(orchestrator: FileIndexerOrchestrator):
    """
    Show detailed statistics including database info and system status.
    """
    print_banner()
    
    print("📊 DATABASE STATISTICS")
    print("-" * 60)
    
    try:
        stats = orchestrator.get_detailed_stats()
        
        # Database stats
        db = stats['database']
        print(f"Total files indexed: {db['total_files']}")
        print(f"Unique document types: {len(db['by_document_type'])}")
        print(f"Last indexed: {db['last_indexed'] or 'Never'}")
        print(f"Unique tags: {db['unique_tags_count']}")
        
        # Document type breakdown
        if db['by_document_type']:
            print("\n📑 DOCUMENT TYPES:")
            for doc_type, count in list(db['by_document_type'].items())[:10]:
                print(f"  {doc_type:15} : {count:5} files")
        
        # Extension breakdown
        if stats['extensions']:
            print("\n🔧 FILE EXTENSIONS:")
            for ext, count in list(stats['extensions'].items())[:10]:
                print(f"  {ext:10} : {count:5} files")
        
        # Recent files
        if stats['recent_files']:
            print("\n🕒 RECENTLY INDEXED (last 5):")
            for file in stats['recent_files']:
                print(f"  • {file['file_name']:30} ({file['document_type']})")
                print(f"    Indexed: {file['indexed_at']}")
        
        # Orchestrator status
        orch = stats['orchestrator']
        print("\n⚙️  SYSTEM STATUS:")
        print(f"  Running: {orch['is_running']}")
        print(f"  Watcher active: {orch['watcher_active']}")
        if orch['session_stats']['processed'] > 0:
            print(f"  Session indexed: {orch['session_stats']['processed']}")
            print(f"  Session failed: {orch['session_stats']['failed']}")
        
        # Model info
        if stats['model']:
            model = stats['model']
            print("\n🤖 MODEL INFO:")
            print(f"  Text model loaded: {model.get('text_model_loaded', False)}")
            print(f"  Vision model loaded: {model.get('vision_model_loaded', False)}")
            print(f"  GPU enabled: {model.get('use_gpu', False)}")
        
        # Configuration
        config = stats['config']
        print("\n🔧 CONFIGURATION:")
        print(f"  Watched folders: {config['watched_folders']}")
        print(f"  File extensions: {config['file_extensions']}")
        print(f"  Max file size: {config['max_file_size_mb']} MB")
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}", exc_info=True)
        print(f"\n❌ Error getting statistics: {e}")


def run_maintenance_command(args, orchestrator: FileIndexerOrchestrator):
    """
    Run maintenance commands and exit.
    
    Args:
        args: Command line arguments
        orchestrator: Initialized orchestrator
        
    Returns:
        True if should exit, False if should continue
    """
    if args.cleanup:
        print_banner()
        print("🧹 CLEANUP MODE")
        print("-" * 60)
        deleted = orchestrator.cleanup_missing_files()
        print(f"\n✅ Cleanup complete: {deleted} records removed")
        return True
    
    elif args.reindex:
        print_banner()
        print("🔄 FULL REINDEX MODE")
        print("-" * 60)
        print("⚠️  Warning: This will delete all existing indexes and rebuild from scratch.")
        print("   This may take a long time for large directories.")
        print()
        
        response = input("Are you sure you want to continue? (yes/NO): ")
        if response.lower() != 'yes':
            print("Reindex cancelled.")
            return True
        
        # Set flag to skip confirmation in orchestrator
        orchestrator.skip_confirmation = True
        count = orchestrator.reindex_all()
        print(f"\n✅ Reindex complete: {count} files indexed")
        return True
    
    elif args.reindex_file:
        print_banner()
        print("🔄 SINGLE FILE REINDEX MODE")
        print("-" * 60)
        
        file_path = args.reindex_file
        if not Path(file_path).exists():
            print(f"❌ File not found: {file_path}")
            return True
        
        print(f"Reindexing: {file_path}")
        success = orchestrator.reindex_file(file_path)
        
        if success:
            print(f"✅ Successfully reindexed: {file_path}")
        else:
            print(f"❌ Failed to reindex: {file_path}")
        return True
    
    elif args.stats:
        show_detailed_stats(orchestrator)
        return True
    
    return False  # Continue with normal operation


def run_headless(args, orchestrator: FileIndexerOrchestrator):
    """
    Run the indexer in headless mode (no tray).
    """
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Store orchestrator in signal handler for cleanup
    signal_handler.orchestrator = orchestrator
    
    # Run once or continuously
    if args.once:
        logger.info("Running in once mode (scan then exit)")
        orchestrator.index_existing_files()
        orchestrator._print_final_stats()
        logger.info("Scan complete. Exiting.")
    else:
        # Start continuous indexing
        orchestrator.start()
        
        # Keep the main thread alive
        try:
            while orchestrator.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\nInterrupted by user")
            orchestrator.stop()


def run_with_tray(args, orchestrator: FileIndexerOrchestrator):
    """
    Run the indexer with system tray integration.
    """
    try:
        from indexer.tray_app import run_tray_app_with_orchestrator
        
        print_banner()
        print("The indexer will run in the system tray.")
        print("Right-click the tray icon to access controls.")
        print()
        print("To run without tray (headless mode), use --no-tray")
        print("=" * 60)
        print()
        
        # Run the tray application with pre-initialized orchestrator
        run_tray_app_with_orchestrator(orchestrator)
        
    except ImportError as e:
        logger.error(f"Failed to import tray dependencies: {e}")
        logger.error("Please install required packages: pip install pystray Pillow")
        logger.info("Falling back to headless mode...")
        run_headless(args, orchestrator)
    except Exception as e:
        logger.error(f"Error running tray app: {e}")
        logger.info("Falling back to headless mode...")
        run_headless(args, orchestrator)


def main():
    """
    Main entry point for the indexer.
    """
    args = parse_arguments()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Handle maintenance commands that don't need full initialization
    if args.stats:
        # For stats, we can initialize just the database
        try:
            from indexer.config_loader import load_config
            from indexer.database import FileDatabase

            config = load_config(args.config) if args.config else load_config()
            
            db = FileDatabase(config['database_path'])
            stats = db.get_all_stats()
            
            print_banner()
            print("📊 DATABASE STATISTICS")
            print("-" * 60)
            print(f"Total files indexed: {stats['total_files']}")
            print(f"Unique document types: {len(stats['by_document_type'])}")
            print(f"Last indexed: {stats['last_indexed'] or 'Never'}")
            print(f"Unique tags: {stats['unique_tags_count']}")
            
            if stats['by_document_type']:
                print("\n📑 Document types:")
                for doc_type, count in list(stats['by_document_type'].items())[:10]:
                    print(f"  {doc_type}: {count} files")
            return
            
        except Exception as e:
            print(f"Error getting stats: {e}")
            sys.exit(1)
    
    # Initialize orchestrator for other commands
    try:
        logger.info("Initializing orchestrator...")
        orchestrator = FileIndexerOrchestrator(config_path=args.config)
        
        # Check if model is loaded (for non-maintenance commands)
        if not args.cleanup and not args.stats:
            if not orchestrator.tagger.is_loaded():
                logger.warning("⚠️  Text model not loaded! Indexing may not work properly.")
                logger.warning(f"   Expected model at: {orchestrator.config['model_path']}")
                
                if not args.reindex and not args.reindex_file:
                    response = input("\nContinue anyway? (y/N): ")
                    if response.lower() != 'y':
                        logger.info("Exiting...")
                        return
        
        # Run maintenance commands if specified
        if run_maintenance_command(args, orchestrator):
            return
        
        # Run normal operation
        if args.no_tray:
            run_headless(args, orchestrator)
        else:
            run_with_tray(args, orchestrator)
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.info("Please ensure config.json exists and has valid paths")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=args.verbose)
        sys.exit(1)
    
    logger.info("File indexer shutdown complete")


if __name__ == "__main__":
    main()



