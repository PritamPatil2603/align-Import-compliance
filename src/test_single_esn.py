# ============================================
# FILE: src\test_single_esn.py
# Test system with ONE specific ESN folder first
# ============================================

import asyncio
import logging
from datetime import datetime
from pathlib import Path
import json

from config import SystemConfig
from google_services import GoogleServicesManager
from invoice_processor import InvoiceProcessor
from main import ComplianceSystemOrchestrator

class SingleESNTester:
    """Test the system with one specific ESN folder"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.google_manager = GoogleServicesManager(
            self.config.GOOGLE_CREDENTIALS_PATH,
            self.config.GOOGLE_SHEETS_ID
        )
        self.invoice_processor = InvoiceProcessor(self.config)
        
        # Setup logging for detailed output
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)
    
    def list_available_esns(self):
        """Show all available ESN folders for user to choose from"""
        print("🔍 FINDING ALL AVAILABLE ESN FOLDERS")
        print("=" * 60)
        
        try:
            esn_folders = self.google_manager.get_all_esn_folders()
            
            if not esn_folders:
                print("❌ No ESN folders found!")
                print("\nTroubleshooting:")
                print("1. Check if folders start with 'AE' followed by numbers")
                print("2. Verify Google Drive access permissions")
                return []
            
            print(f"✅ Found {len(esn_folders)} ESN folders:")
            print()
            
            # Show folders in organized format
            for i, folder in enumerate(esn_folders, 1):
                print(f"{i:3d}. {folder['esn']}")
                if i % 20 == 0:  # Pause every 20 entries
                    input("Press Enter to see more...")
            
            print(f"\n📋 Total: {len(esn_folders)} ESN folders available")
            return esn_folders
            
        except Exception as e:
            print(f"❌ Error finding ESN folders: {e}")
            return []
    
    async def test_specific_esn(self, target_esn: str = None):
        """Test system with one specific ESN"""
        
        print("🎯 TESTING SINGLE ESN FOLDER")
        print("=" * 60)
        
        # Step 1: Find available ESNs
        esn_folders = self.google_manager.get_all_esn_folders()
        
        if not esn_folders:
            print("❌ No ESN folders found")
            return None
        
        # Step 2: Select ESN to test
        if target_esn:
            # User specified an ESN
            selected_esn = next((esn for esn in esn_folders if esn['esn'] == target_esn), None)
            if not selected_esn:
                print(f"❌ ESN '{target_esn}' not found")
                print(f"Available ESNs: {[esn['esn'] for esn in esn_folders[:5]]}...")
                return None
        else:
            # Let user choose
            print("Available ESNs:")
            for i, esn_info in enumerate(esn_folders[:10], 1):
                print(f"{i}. {esn_info['esn']}")
            
            if len(esn_folders) > 10:
                print(f"... and {len(esn_folders) - 10} more")
            
            print(f"\n🎯 Using first ESN for testing: {esn_folders[0]['esn']}")
            selected_esn = esn_folders[0]
        
        esn = selected_esn['esn']
        folder_id = selected_esn['folder_id']
        
        print(f"\n🔬 TESTING ESN: {esn}")
        print("=" * 40)
        
        # Step 3: Check Google Sheets data
        print("📊 Step 1: Checking Google Sheets...")
        declared_amount = self.google_manager.get_esn_declared_amount(esn)
        
        if declared_amount is None:
            print(f"❌ ESN {esn} not found in Google Sheets")
            print("💡 Check if the ESN exists in your spreadsheet")
            return None
        
        print(f"✅ Declared amount: ${declared_amount:,.2f}")
        
        # Step 4: Find invoice files
        print("\n📁 Step 2: Finding invoice files...")
        invoice_files = self.google_manager.get_commercial_invoices_files(folder_id)
        
        if not invoice_files:
            print("❌ No invoice files found")
            print("💡 Check if 'COMMERCIAL INVOICES' subfolder exists")
            return None
        
        print(f"✅ Found {len(invoice_files)} PDF files:")
        for file_info in invoice_files:
            size_mb = int(file_info.get('size', 0)) / (1024*1024) if file_info.get('size') else 0
            print(f"   📄 {file_info['name']} ({size_mb:.1f}MB)")
        
        # Step 5: Download and process invoices
        print(f"\n⚡ Step 3: Processing {len(invoice_files)} invoices...")
        
        # Create temp directory
        temp_dir = Path(self.config.TEMP_DIR) / esn
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        print("📥 Downloading files...")
        
        for file_info in invoice_files:
            local_path = temp_dir / file_info['name']
            if self.google_manager.download_file(file_info['id'], str(local_path)):
                downloaded_files.append(str(local_path))
                print(f"   ✅ Downloaded: {file_info['name']}")
            else:
                print(f"   ❌ Failed: {file_info['name']}")
        
        if not downloaded_files:
            print("❌ No files downloaded successfully")
            return None
        
        # Step 6: Extract data from invoices
        print(f"\n🔍 Step 4: Extracting data from {len(downloaded_files)} PDFs...")
        extracted_invoices = await self.invoice_processor.process_esn_invoices(esn, downloaded_files)
        
        # Step 7: Show detailed results
        print("\n📋 EXTRACTION RESULTS:")
        print("=" * 50)
        
        total_calculated = 0
        successful_extractions = 0
        
        for i, invoice in enumerate(extracted_invoices, 1):
            confidence_icon = {
                "HIGH": "🟢",
                "MEDIUM": "🟡", 
                "LOW": "🟠",
                "ERROR": "🔴"
            }.get(invoice.confidence_level.value, "❓")
            
            print(f"{i}. {confidence_icon} {invoice.invoice_number}")
            print(f"   Company: {invoice.company_name}")
            print(f"   Amount: ${invoice.total_usd_amount:,.2f} {invoice.currency}")
            print(f"   Confidence: {invoice.confidence_level.value}")
            
            if invoice.amount_source_text:
                print(f"   Source: {invoice.amount_source_text[:100]}...")
            
            if invoice.extraction_notes:
                print(f"   Notes: {invoice.extraction_notes}")
            
            if invoice.confidence_level.value != "ERROR":
                total_calculated += invoice.total_usd_amount
                successful_extractions += 1
            
            print()
        
        # Step 8: Calculate compliance
        print("🎯 COMPLIANCE ANALYSIS:")
        print("=" * 30)
        print(f"Declared Amount:  ${declared_amount:,.2f}")
        print(f"Calculated Total: ${total_calculated:,.2f}")
        print(f"Difference:       ${abs(declared_amount - total_calculated):,.2f}")
        
        if declared_amount > 0:
            percentage_diff = abs(declared_amount - total_calculated) / declared_amount * 100
            print(f"Percentage Diff:  {percentage_diff:.2f}%")
            
            if percentage_diff <= self.config.TOLERANCE_PERCENTAGE:
                print("✅ STATUS: MATCH (Within tolerance)")
            else:
                print("❌ STATUS: MISMATCH (Exceeds tolerance)")
        else:
            print("⚠️  STATUS: Cannot calculate (zero declared amount)")
        
        print(f"\nSuccessful Extractions: {successful_extractions}/{len(extracted_invoices)}")
        print(f"Success Rate: {successful_extractions/len(extracted_invoices)*100:.1f}%")
        
        # Step 9: Save detailed results for manual review
        result_data = {
            "esn": esn,
            "test_timestamp": datetime.now().isoformat(),
            "declared_amount": float(declared_amount),
            "calculated_amount": float(total_calculated),
            "difference": float(abs(declared_amount - total_calculated)),
            "percentage_difference": float(percentage_diff) if declared_amount > 0 else 0,
            "extracted_invoices": [
                {
                    "invoice_number": inv.invoice_number,
                    "company_name": inv.company_name,
                    "amount": float(inv.total_usd_amount),
                    "currency": inv.currency,
                    "confidence": inv.confidence_level.value,
                    "source_text": inv.amount_source_text,
                    "notes": inv.extraction_notes
                }
                for inv in extracted_invoices
            ]
        }
        
        # Save result
        result_file = Path(self.config.OUTPUT_DIR) / f"test_{esn}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(result_file, 'w') as f:
            json.dump(result_data, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {result_file}")
        
        # Cleanup
        for file_path in downloaded_files:
            try:
                Path(file_path).unlink()
            except:
                pass
        try:
            temp_dir.rmdir()
        except:
            pass
        
        # Step 10: Manual validation guidance
        print("\n🔍 MANUAL VALIDATION STEPS:")
        print("=" * 35)
        print("1. Open the PDFs in Google Drive manually")
        print("2. Find the total USD amount in each invoice")
        print("3. Add them up manually")
        print("4. Compare with system calculation above")
        print("5. Verify the declared amount in Google Sheets")
        
        if percentage_diff > 5:  # High discrepancy
            print("\n⚠️  HIGH DISCREPANCY DETECTED!")
            print("Recommended actions:")
            print("- Manually verify invoice amounts")
            print("- Check currency conversions")
            print("- Review extraction confidence levels")
        
        return result_data
    
    async def run_interactive_test(self):
        """Interactive test runner"""
        
        print("🧪 SINGLE ESN TESTING TOOL")
        print("=" * 50)
        print("This tool will test the system with ONE ESN folder")
        print("to validate accuracy before running on all folders.")
        print()
        
        try:
            # Validate configuration first
            if not self.config.validate():
                return
            
            # Option 1: List all ESNs and let user choose
            print("Choose testing option:")
            print("1. See all available ESNs and choose one")
            print("2. Test a specific ESN (if you know the name)")
            print("3. Test first available ESN automatically")
            
            choice = input("\nEnter choice (1/2/3): ").strip()
            
            if choice == "1":
                # Show all ESNs
                esn_folders = self.list_available_esns()
                if not esn_folders:
                    return
                
                esn_choice = input(f"\nEnter ESN name to test (or press Enter for first): ").strip()
                if esn_choice:
                    result = await self.test_specific_esn(esn_choice)
                else:
                    result = await self.test_specific_esn(esn_folders[0]['esn'])
                    
            elif choice == "2":
                # Test specific ESN
                target_esn = input("Enter ESN name (e.g., AE900683929): ").strip()
                if target_esn:
                    result = await self.test_specific_esn(target_esn)
                else:
                    print("❌ No ESN provided")
                    return
                    
            elif choice == "3":
                # Test first ESN automatically
                result = await self.test_specific_esn()
                
            else:
                print("❌ Invalid choice")
                return
            
            # Show final recommendation
            if result:
                print("\n" + "=" * 60)
                print("🎯 TEST COMPLETED!")
                print("=" * 60)
                
                if result.get('percentage_difference', 100) <= 5:
                    print("✅ RECOMMENDATION: System looks accurate!")
                    print("   Ready to proceed with full processing.")
                    print("\n   Next step: Run 'python main.py' for all ESNs")
                else:
                    print("⚠️  RECOMMENDATION: Review system accuracy")
                    print("   High discrepancy detected. Manual verification needed.")
                    print("\n   Next step: Investigate extraction issues")
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()

# ============================================
# MAIN EXECUTION
# ============================================

async def main():
    """Main function - run interactive test"""
    tester = SingleESNTester()
    await tester.run_interactive_test()

if __name__ == "__main__":
    asyncio.run(main())