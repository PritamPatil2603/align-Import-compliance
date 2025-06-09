import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class SystemConfig:
    """Production system configuration"""
    
    # API Keys
    LLAMA_CLOUD_API_KEY: str = os.getenv("LLAMA_CLOUD_API_KEY", "")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    
    # Google Services
    GOOGLE_CREDENTIALS_PATH: str = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/credentials.json")
    GOOGLE_SHEETS_ID: str = os.getenv("GOOGLE_SHEETS_ID", "")
    
    # Processing Settings
    MAX_CONCURRENT_PDFS: int = int(os.getenv("MAX_CONCURRENT_PDFS", "6"))
    TOLERANCE_PERCENTAGE: float = float(os.getenv("TOLERANCE_PERCENTAGE", "1.0"))
    
    # Directories
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data/reports")
    TEMP_DIR: str = os.getenv("TEMP_DIR", "data/temp")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    def __post_init__(self):
        """Create directories and validate config"""
        # Create directories
        Path(self.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        Path(self.TEMP_DIR).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)
    
    def validate(self) -> bool:
        """Validate all configuration settings"""
        errors = []
        
        if not self.LLAMA_CLOUD_API_KEY or not self.LLAMA_CLOUD_API_KEY.startswith("llx-"):
            errors.append("Invalid LLAMA_CLOUD_API_KEY - should start with 'llx-'")
        
        if not self.OPENAI_API_KEY or not self.OPENAI_API_KEY.startswith("sk-"):
            errors.append("Invalid OPENAI_API_KEY - should start with 'sk-'")
        
        if not self.GOOGLE_SHEETS_ID:
            errors.append("GOOGLE_SHEETS_ID is required")
        
        if not os.path.exists(self.GOOGLE_CREDENTIALS_PATH):
            errors.append(f"Google credentials file not found: {self.GOOGLE_CREDENTIALS_PATH}")
        
        if errors:
            print("❌ Configuration Errors:")
            for error in errors:
                print(f"   - {error}")
            return False
        
        print("✅ Configuration validated successfully")
        return True