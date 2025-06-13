import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class SystemConfig:
    """System configuration settings"""
    
    # Existing API keys
    LLAMA_CLOUD_API_KEY: str = os.getenv('LLAMA_CLOUD_API_KEY')
    OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY')
    
    # Google Services
    GOOGLE_SHEETS_ID: str = os.getenv('GOOGLE_SHEETS_ID')
    GOOGLE_CREDENTIALS_PATH: str = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials/credentials.json')
    
    # Directory settings
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'data/reports')
    TEMP_DIR: str = os.getenv('TEMP_DIR', 'data/temp')
    
    # Processing settings
    MAX_CONCURRENT_PDFS: int = int(os.getenv('MAX_CONCURRENT_PDFS', '3'))
    TOLERANCE_PERCENTAGE: float = float(os.getenv('TOLERANCE_PERCENTAGE', '1.0'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # MongoDB Atlas settings (simplified - credentials in URI)
    MONGODB_URI: str = os.getenv('MONGODB_URI', '')
    MONGODB_DATABASE: str = os.getenv('MONGODB_DATABASE', 'alignai-staging-db')
    
    # Maesa organization ID
    MAESA_ORGANIZATION_ID: str = os.getenv('MAESA_ORGANIZATION_ID', 'dff4dbb5-e2cb-49b3-8ae4-082418ac1db2')
    
    def __post_init__(self):
        """Create directories if they don't exist"""
        Path(self.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.TEMP_DIR).mkdir(parents=True, exist_ok=True)
    
    @property
    def mongodb_configured(self) -> bool:
        """Check if MongoDB is properly configured"""
        return bool(self.MONGODB_URI and self.MONGODB_DATABASE)