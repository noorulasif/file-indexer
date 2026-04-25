"""
Configuration loader for the File Indexer application.
Handles loading, validating, and creating default configuration.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, List


def get_project_root() -> Path:
    """
    Get the project root directory (where config.json should live).
    
    Returns:
        Path to project root directory
    """
    # Assuming this file is in indexer/ directory
    current_file = Path(__file__).resolve()
    return current_file.parent.parent


def get_config_path() -> Path:
    """
    Get the path to config.json in the project root.
    
    Returns:
        Path to config.json
    """
    return get_project_root() / "config.json"


def get_default_config() -> Dict[str, Any]:
    """
    Create the default configuration dictionary.
    
    Returns:
        Dictionary with default configuration values
    """
    return {
        "watched_folders": [
            "~/Documents",
            "~/Downloads"
        ],
        "file_extensions": [
            ".txt",
            ".md",
            ".pdf",
            ".docx",
            ".xlsx",
            ".csv",
            ".jpg",
            ".jpeg",
            ".png"
        ],
        "model_path": "models/phi-4-mini-q4.gguf",
        "vision_model_path": "models/qwen2.5-vl-3b-q4.gguf",
        "database_path": "file_index.db",
        "use_gpu": False,
        "gpu_layers": 0,
        "max_file_size_mb": 50,
        "indexer_threads": 1
    }


def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate that the configuration has all required keys and correct types.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if valid, raises ValueError if invalid
        
    Raises:
        ValueError: If configuration is missing required keys or has invalid types
    """
    required_keys = {
        "watched_folders": list,
        "file_extensions": list,
        "model_path": str,
        "vision_model_path": str,
        "database_path": str,
        "use_gpu": bool,
        "gpu_layers": int,
        "max_file_size_mb": (int, float),
        "indexer_threads": int
    }
    
    for key, expected_type in required_keys.items():
        if key not in config:
            raise ValueError(f"Missing required configuration key: '{key}'")
        
        # Handle multiple possible types
        if isinstance(expected_type, tuple):
            if not isinstance(config[key], expected_type):
                raise ValueError(
                    f"Key '{key}' should be one of {expected_type}, "
                    f"got {type(config[key]).__name__}"
                )
        else:
            if not isinstance(config[key], expected_type):
                raise ValueError(
                    f"Key '{key}' should be of type {expected_type.__name__}, "
                    f"got {type(config[key]).__name__}"
                )
    
    # Additional validation for specific values
    if config["gpu_layers"] < 0:
        raise ValueError("gpu_layers must be non-negative")
    
    if config["max_file_size_mb"] <= 0:
        raise ValueError("max_file_size_mb must be positive")
    
    if config["indexer_threads"] < 1:
        raise ValueError("indexer_threads must be at least 1")
    
    if not config["watched_folders"]:
        raise ValueError("watched_folders cannot be empty")
    
    if not config["file_extensions"]:
        raise ValueError("file_extensions cannot be empty")
    
    return True


def expand_paths(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expand user home directory (~) in path fields.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configuration with expanded paths (creates a copy)
    """
    expanded_config = config.copy()
    
    # Expand watched folders
    if "watched_folders" in expanded_config:
        expanded_config["watched_folders"] = [
            os.path.expanduser(folder) for folder in expanded_config["watched_folders"]
        ]
    
    # Expand model paths
    if "model_path" in expanded_config:
        expanded_config["model_path"] = os.path.expanduser(expanded_config["model_path"])
    
    if "vision_model_path" in expanded_config:
        expanded_config["vision_model_path"] = os.path.expanduser(
            expanded_config["vision_model_path"]
        )
    
    # Expand database path
    if "database_path" in expanded_config:
        expanded_config["database_path"] = os.path.expanduser(
            expanded_config["database_path"]
        )
    
    return expanded_config


def create_default_config() -> None:
    """
    Create a default config.json file in the project root.
    """
    config_path = get_config_path()
    default_config = get_default_config()
    
    # Create the directory if it doesn't exist
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the default configuration
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=2)
    
    print(f"Created default configuration at: {config_path}")


def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.json in the project root.
    If config.json doesn't exist, create it with defaults.
    
    Returns:
        Dictionary containing the configuration
        
    Raises:
        FileNotFoundError: If config.json cannot be created
        json.JSONDecodeError: If config.json is malformed
        ValueError: If configuration validation fails
    """
    config_path = get_config_path()
    
    # Create default config if it doesn't exist
    if not config_path.exists():
        create_default_config()
    
    # Read and parse the configuration
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Failed to parse config.json: {e.msg}",
            e.doc,
            e.pos
        )
    
    # Validate the configuration
    validate_config(config)
    
    # Expand paths with user home directory
    expanded_config = expand_paths(config)
    
    return expanded_config


def save_config(config: Dict[str, Any]) -> None:
    """
    Save configuration to config.json.
    
    Args:
        config: Configuration dictionary to save
    """
    config_path = get_config_path()
    
    # Validate before saving
    validate_config(config)
    
    # Create directory if needed
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to file
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)


# Example usage (for testing)
if __name__ == "__main__":
    # Test loading configuration
    try:
        cfg = load_config()
        print("Configuration loaded successfully:")
        print(json.dumps(cfg, indent=2))
        
        # Test saving (optional - commented out)
        # cfg["max_file_size_mb"] = 100
        # save_config(cfg)
        
    except Exception as e:
        print(f"Error loading configuration: {e}")



