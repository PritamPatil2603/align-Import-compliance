"""
Spanish Invoice Data Extractor - Entry Point

This script extracts data from Spanish commercial invoices stored in Google Drive
for US import compliance auditing purposes.

Features:
- Processes all ESN folders automatically
- Extracts multiple line items per invoice
- Exports to JSON, CSV, and Excel formats
- Provides audit-ready data structure

Usage:
    python run_spanish_extractor.py

Requirements:
    - .env file with API keys configured
    - Google Drive credentials in credentials/credentials.json
    - ESN folders containing COMMERCIAL INVOICES subfolders
"""

import sys
import os
import asyncio
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import the main processor
from spanish_extractor.main import main

def check_requirements():
    """Check if all requirements are met before starting"""
    
    print("🔍 Checking requirements...")
    
    # Check .env file
    env_file = project_root / '.env'
    if not env_file.exists():
        print("❌ .env file not found. Please copy .env.example to .env and configure it.")
        return False
    
    # Check Google credentials
    credentials_file = project_root / 'credentials' / 'credentials.json'
    if not credentials_file.exists():
        print("❌ Google credentials not found. Please place credentials.json in the credentials/ folder.")
        return False
    
    # Check required environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = ['LLAMA_CLOUD_API_KEY', 'OPENAI_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please configure them in your .env file.")
        return False
    
    print("✅ All requirements met!")
    return True

def display_banner():
    """Display application banner"""
    
    banner = """
██╗███╗   ██╗██╗   ██╗ ██████╗ ██╗ ██████╗███████╗    
██║████╗  ██║██║   ██║██╔═══██╗██║██╔════╝██╔════╝    
██║██╔██╗ ██║██║   ██║██║   ██║██║██║     █████╗      
██║██║╚██╗██║╚██╗ ██╔╝██║   ██║██║██║     ██╔══╝      
██║██║ ╚████║ ╚████╔╝ ╚██████╔╝██║╚██████╗███████╗    
╚═╝╚═╝  ╚═══╝  ╚═══╝   ╚═════╝ ╚═╝ ╚═════╝╚══════╝    
                                                      
███████╗██╗  ██╗████████╗██████╗  █████╗  ██████╗████████╗ ██████╗ ██████╗ 
██╔════╝╚██╗██╔╝╚══██╔══╝██╔══██╗██╔══██╗██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗
█████╗   ╚███╔╝    ██║   ██████╔╝███████║██║        ██║   ██║   ██║██████╔╝
██╔══╝   ██╔██╗    ██║   ██╔══██╗██╔══██║██║        ██║   ██║   ██║██╔══██╗
███████╗██╔╝ ██╗   ██║   ██║  ██║██║  ██║╚██████╗   ██║   ╚██████╔╝██║  ██║
╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
    """
    
    print(banner)
    print("🇪🇸 Spanish Invoice Data Extractor v1.0")
    print("📋 US Import Compliance Audit System")
    print("=" * 80)
    print()

def main_entry():
    """Main entry point with error handling"""
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Display banner
    display_banner()
    
    # Check requirements
    if not check_requirements():
        print("\n❌ Requirements check failed. Please fix the issues above and try again.")
        input("Press Enter to exit...")
        return
    
    try:
        # Create required directories
        data_dirs = ['data/spanish_cache', 'data/spanish_exports', 'data/spanish_temp', 'logs']
        for dir_path in data_dirs:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        print("📁 Directory structure created")
        print()
        
        # Run the extractor
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Extraction cancelled by user")
        print("Data that was already processed has been saved.")
        
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        print("Please check the log files for detailed error information.")
        
    finally:
        print("\n" + "=" * 80)
        print("🏁 Spanish Invoice Extractor session ended")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main_entry()