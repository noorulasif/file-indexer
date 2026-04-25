#!/usr/bin/env python3
"""
Main entry point for the File Indexer background service.
Starts the indexer with optional system tray integration.
"""

import sys
import time
import signal
import argparse
import logging
from pathlib import Path

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
  
  # Run with custom config file
  python run_indexer.py --config /path/to/custom_config.json
  
  # Run once (no watching, just scan and exit)
  python run_indexer.py --once
  
  # Show status of indexer
  python run_indexer.py --status
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
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current indexer status and exit'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    return parser.parse_args()


def show_status():
    """
    Show current indexer status by checking the database.
    """
    try:
        from indexer.config_loader import load_config
        from indexer.database import FileDatabase
        
        print("\n" + "=" * 60)
        print("FILE INDEXER STATUS")
        print("=" * 60)
        
        # Load config
        config = load_config()
        print(f"\n📁 Database: {config['database_path']}")
        
        # Connect to database
        db = FileDatabase(config['database_path'])
        stats = db.get_all_stats()
        
        print(f"\n📊 INDEX STATISTICS:")
        print(f"  Total files indexed: {stats['total_files']}")
        print(f"  Unique document types: {len(stats['by_document_type'])}")
        print(f"  Last indexed: {stats['last_indexed'] or 'Never'}")
        print(f"  Total unique tags: {stats['unique_tags_count']}")
        
        print(f"\n📑 DOCUMENT TYPES:")
        if stats['by_document_type']:
            for doc_type, count in list(stats['by_document_type'].items())[:10]:
                print(f"  {doc_type}: {count} files")
        else:
            print("  No documents indexed yet")
        
        print(f"\n🔍 RECENTLY INDEXED FILES (last 5):")
        recent = db.get_recent_files(limit=5)
        if recent:
            for file in recent:
                print(f"  📄 {file['file_name']} ({file['document_type']})")
                print(f"     Indexed: {file['indexed_at']}")
        else:
            print("  No files indexed yet")
        
        print(f"\n🔧 CONFIGURATION:")
        print(f"  Watched folders: {len(config['watched_folders'])}")
        print(f"  File extensions: {', '.join(config['file_extensions'][:5])}{'...' if len(config['file_extensions']) > 5 else ''}")
        print(f"  Max file size: {config['max_file_size_mb']} MB")
        print(f"  GPU enabled: {config.get('use_gpu', False)}")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"Error getting status: {e}")
        sys.exit(1)


def run_headless(args):
    """
    Run the indexer in headless mode (no tray).
    """
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create orchestrator
        logger.info("Initializing orchestrator...")
        orchestrator = FileIndexerOrchestrator(config_path=args.config)
        
        # Store orchestrator in signal handler for cleanup
        signal_handler.orchestrator = orchestrator
        
        # Check if model is loaded
        if not orchestrator.tagger.is_loaded():
            logger.warning("⚠️  Text model not loaded! Indexing may not work properly.")
            logger.warning(f"   Expected model at: {orchestrator.config['model_path']}")
            
            response = input("\nContinue anyway? (y/N): ")
            if response.lower() != 'y':
                logger.info("Exiting...")
                return
        
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
        
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.info("Please ensure config.json exists and has valid paths")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=args.verbose)
        sys.exit(1)


def run_with_tray(args):
    """
    Run the indexer with system tray integration.
    """
    try:
        from indexer.tray_app import run_tray_app
        
        print("=" * 60)
        print("FILE INDEXER - System Tray Mode")
        print("=" * 60)
        print()
        print("The indexer will run in the system tray.")
        print("Right-click the tray icon to access controls.")
        print()
        print("To run without tray (headless mode), use --no-tray")
        print("=" * 60)
        print()
        
        # Run the tray application
        run_tray_app(config_path=args.config)
        
    except ImportError as e:
        logger.error(f"Failed to import tray dependencies: {e}")
        logger.error("Please install required packages: pip install pystray Pillow")
        logger.info("Falling back to headless mode...")
        run_headless(args)
    except Exception as e:
        logger.error(f"Error running tray app: {e}")
        logger.info("Falling back to headless mode...")
        run_headless(args)


def main():
    """
    Main entry point for the indexer.
    """
    args = parse_arguments()
    
    # Handle status request
    if args.status:
        show_status()
        return
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Run with or without tray
    if args.no_tray or args.once:
        # Headless mode (no tray)
        run_headless(args)
    else:
        # System tray mode
        run_with_tray(args)
    
    logger.info("File indexer shutdown complete")


if __name__ == "__main__":
    main()



