import os
from pathlib import Path
from dataclasses import dataclass

@dataclass
class SpanishExtractorConfig:
    """Configuration for Spanish Invoice Extractor"""
    
    # API Keys
    LLAMA_CLOUD_API_KEY: str = os.getenv('LLAMA_CLOUD_API_KEY')
    OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY')
    
    # Google Drive
    GOOGLE_CREDENTIALS_PATH: str = "credentials/credentials.json"
    
    # Processing settings
    MAX_CONCURRENT_PDFS: int = 3
    TIMEOUT_SECONDS: int = 60  # Reduced from 120
    
    # Directories
    CACHE_DIR: str = "data/spanish_cache"
    EXPORT_DIR: str = "data/spanish_exports" 
    TEMP_DIR: str = "data/spanish_temp"
    LOGS_DIR: str = "logs"
    
    # Export settings
    EXPORT_JSON: bool = True
    EXPORT_CSV: bool = True
    EXPORT_EXCEL: bool = True
    
    def __post_init__(self):
        """Create directories and validate config"""
        # Create all required directories
        for dir_path in [self.CACHE_DIR, self.EXPORT_DIR, self.TEMP_DIR, self.LOGS_DIR]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        # Validate required API keys
        if not self.LLAMA_CLOUD_API_KEY:
            raise ValueError("LLAMA_CLOUD_API_KEY environment variable is required")
        if not self.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        # Validate credentials file
        if not os.path.exists(self.GOOGLE_CREDENTIALS_PATH):
            raise ValueError(f"Google credentials file not found: {self.GOOGLE_CREDENTIALS_PATH}")