# ============================================
# FILE: src/test_single_esn.py (ENHANCED VERSION)
# Now SAVES all test results to files for audit trails
# ============================================

import asyncio
import logging
import time
import json
import ssl
import socket
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from config import SystemConfig
from google_services import GoogleServicesManager
from invoice_processor import InvoiceProcessor
from models import CommercialInvoiceData, ConfidenceLevel

class ResultSaver:
    """Handles saving test results in multiple formats"""
    
    def __init__(self, config: SystemConfig):
        self.config = config
        self.output_dir = Path(config.OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.output_dir / "tests").mkdir(exist_ok=True)
        (self.output_dir / "detailed").mkdir(exist_ok=True)
        (self.output_dir / "excel").mkdir(exist_ok=True)
    
    def save_test_result(self, result_data: Dict, test_type: str = "single_esn") -> Dict[str, str]:
        """Save test result in multiple formats"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        esn = result_data.get('esn', 'UNKNOWN')
        
        # File paths
        json_file = self.output_dir / "tests" / f"{test_type}_{esn}_{timestamp}.json"
        excel_file = self.output_dir / "excel" / f"{test_type}_{esn}_{timestamp}.xlsx"
        detailed_file = self.output_dir / "detailed" / f"{test_type}_{esn}_{timestamp}_detailed.json"
        
        saved_files = {}
        
        try:
            # 1. Save JSON summary
            summary_data = {
                "test_metadata": {
                    "test_type": test_type,
                    "timestamp": timestamp,
                    "esn": esn,
                    "test_date": datetime.now().isoformat(),
                    "system_version": "production_v1.0"
                },
                "results": {
                    "esn": result_data.get('esn'),
                    "declared_amount": result_data.get('declared_amount'),
                    "calculated_amount": result_data.get('calculated_amount'),
                    "difference": result_data.get('difference'),
                    "percentage_difference": result_data.get('percentage_difference'),
                    "is_compliant": result_data.get('is_compliant'),
                    "status": "COMPLIANT" if result_data.get('is_compliant') else "NON-COMPLIANT"
                },
                "performance": {
                    "total_invoices": result_data.get('total_invoices'),
                    "successful_extractions": result_data.get('successful_extractions'),
                    "failed_extractions": result_data.get('failed_extractions', 0),
                    "ai_processing_time": result_data.get('ai_processing_time'),
                    "avg_time_per_pdf": result_data.get('ai_processing_time', 0) / max(result_data.get('total_invoices', 1), 1),
                    "success_rate": (result_data.get('successful_extractions', 0) / max(result_data.get('total_invoices', 1), 1)) * 100
                }
            }
            
            with open(json_file, 'w') as f:
                json.dump(summary_data, f, indent=2, default=str)
            saved_files['json'] = str(json_file)
            
            # 2. Save detailed JSON (with all invoice details)
            detailed_data = {
                **summary_data,
                "detailed_invoice_results": result_data.get('invoice_details', []),
                "raw_result_data": result_data
            }
            
            with open(detailed_file, 'w') as f:
                json.dump(detailed_data, f, indent=2, default=str)
            saved_files['detailed'] = str(detailed_file)
            
            # 3. Save Excel report
            self._create_excel_report(result_data, excel_file, summary_data)
            saved_files['excel'] = str(excel_file)
            
            return saved_files
            
        except Exception as e:
            logging.error(f"Error saving results: {e}")
            return {}
    
    def _create_excel_report(self, result_data: Dict, excel_file: Path, summary_data: Dict):
        """Create comprehensive Excel report"""
        
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                
                # Sheet 1: Summary
                summary_df = pd.DataFrame([{
                    'ESN': result_data.get('esn'),
                    'Test_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Declared_Amount': f"${result_data.get('declared_amount', 0):,.2f}",
                    'Calculated_Amount': f"${result_data.get('calculated_amount', 0):,.2f}",
                    'Difference': f"${result_data.get('difference', 0):,.2f}",
                    'Percentage_Difference': f"{result_data.get('percentage_difference', 0):.2f}%",
                    'Status': "COMPLIANT" if result_data.get('is_compliant') else "NON-COMPLIANT",
                    'Total_PDFs': result_data.get('total_invoices', 0),
                    'Successful_Extractions': result_data.get('successful_extractions', 0),
                    'AI_Processing_Time': f"{result_data.get('ai_processing_time', 0):.1f}s",
                    'Avg_Time_Per_PDF': f"{result_data.get('ai_processing_time', 0) / max(result_data.get('total_invoices', 1), 1):.1f}s"
                }])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Sheet 2: Invoice Details
                invoice_details = result_data.get('invoice_details', [])
                if invoice_details:
                    invoice_df = pd.DataFrame(invoice_details)
                    invoice_df.to_excel(writer, sheet_name='Invoice_Details', index=False)
                
                # Sheet 3: Performance Metrics
                performance_data = [{
                    'Metric': 'Total Processing Time',
                    'Value': f"{result_data.get('ai_processing_time', 0):.1f}s"
                }, {
                    'Metric': 'Average per PDF',
                    'Value': f"{result_data.get('ai_processing_time', 0) / max(result_data.get('total_invoices', 1), 1):.1f}s"
                }, {
                    'Metric': 'Success Rate',
                    'Value': f"{(result_data.get('successful_extractions', 0) / max(result_data.get('total_invoices', 1), 1)) * 100:.1f}%"
                }, {
                    'Metric': 'Accuracy',
                    'Value': f"{100 - result_data.get('percentage_difference', 0):.2f}%"
                }]
                
                performance_df = pd.DataFrame(performance_data)
                performance_df.to_excel(writer, sheet_name='Performance', index=False)
                
        except Exception as e:
            logging.error(f"Error creating Excel report: {e}")
    
    def save_benchmark_results(self, benchmark_data: List[Dict]) -> str:
        """Save benchmark results comparing multiple ESNs"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        benchmark_file = self.output_dir / f"benchmark_results_{timestamp}.xlsx"
        
        try:
            with pd.ExcelWriter(benchmark_file, engine='openpyxl') as writer:
                
                # Summary sheet
                summary_data = []
                for result in benchmark_data:
                    summary_data.append({
                        'ESN': result.get('esn'),
                        'Declared_Amount': result.get('declared_amount'),
                        'Calculated_Amount': result.get('calculated_amount'),
                        'Difference': result.get('difference'),
                        'Percentage_Diff': result.get('percentage_difference'),
                        'Status': 'COMPLIANT' if result.get('is_compliant') else 'NON-COMPLIANT',
                        'PDFs_Processed': result.get('total_invoices'),
                        'Success_Rate': f"{(result.get('successful_extractions', 0) / max(result.get('total_invoices', 1), 1)) * 100:.1f}%",
                        'Processing_Time': result.get('ai_processing_time'),
                        'Avg_Per_PDF': result.get('ai_processing_time', 0) / max(result.get('total_invoices', 1), 1)
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Benchmark_Summary', index=False)
                
                # Overall statistics
                total_pdfs = sum(r.get('total_invoices', 0) for r in benchmark_data)
                total_time = sum(r.get('ai_processing_time', 0) for r in benchmark_data)
                avg_accuracy = sum(100 - r.get('percentage_difference', 0) for r in benchmark_data) / len(benchmark_data) if benchmark_data else 0
                
                stats_data = [{
                    'Metric': 'Total ESNs Tested',
                    'Value': len(benchmark_data)
                }, {
                    'Metric': 'Total PDFs Processed',
                    'Value': total_pdfs
                }, {
                    'Metric': 'Total Processing Time',
                    'Value': f"{total_time:.1f}s"
                }, {
                    'Metric': 'Average Accuracy',
                    'Value': f"{avg_accuracy:.2f}%"
                }, {
                    'Metric': 'Average Time per PDF',
                    'Value': f"{total_time / max(total_pdfs, 1):.1f}s"
                }]
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Overall_Statistics', index=False)
            
            return str(benchmark_file)
            
        except Exception as e:
            logging.error(f"Error saving benchmark results: {e}")
            return ""

class RobustCachedGoogleManager:
    """Google Services manager with robust error handling and fallbacks"""
    
    def __init__(self, google_manager: GoogleServicesManager):
        self.google_manager = google_manager
        self._sheets_cache = None
        self._esn_folders_cache = None
        self.logger = logging.getLogger(__name__)
        
        # Configure for more robust connections
        self.max_retries = 3
        self.retry_delay = 2.0
    
    async def get_sheets_cache_with_retry(self) -> Dict[str, float]:
        """Get sheets cache with comprehensive retry logic"""
        
        if self._sheets_cache is not None:
            return self._sheets_cache
        
        self.logger.info("📊 Loading Google Sheets data with retry logic...")
        
        # Try multiple approaches in order of preference
        approaches = [
            ("optimized_pandas", self._load_sheets_optimized),
            ("original_method", self._load_sheets_original),
            ("fallback_method", self._load_sheets_fallback)
        ]
        
        for approach_name, method in approaches:
            for attempt in range(self.max_retries):
                try:
                    self.logger.info(f"Attempting {approach_name} (attempt {attempt + 1}/{self.max_retries})")
                    
                    start_time = time.time()
                    result = await method()
                    duration = time.time() - start_time
                    
                    if result:
                        self.logger.info(f"✅ Success with {approach_name}: {len(result)} ESNs in {duration:.2f}s")
                        self._sheets_cache = result
                        return result
                    
                except Exception as e:
                    self.logger.warning(f"❌ {approach_name} attempt {attempt + 1} failed: {e}")
                    
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        self.logger.info(f"Waiting {wait_time:.1f}s before retry...")
                        await asyncio.sleep(wait_time)
        
        # If all methods fail, return empty cache but continue
        self.logger.error("❌ All sheet loading methods failed, using empty cache")
        self._sheets_cache = {}
        return self._sheets_cache
    
    async def _load_sheets_optimized(self) -> Dict[str, float]:
        """Original optimized method with better error handling"""
        try:
            # Read spreadsheet with timeout
            range_name = "Sheet1!A:Z"
            result = self.google_manager.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.google_manager.sheets_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return {}
            
            # Process with pandas
            df = pd.DataFrame(values[1:], columns=values[0])
            df.columns = df.columns.str.strip()
            
            esn_column = 'Entry Summary Number'
            amount_column = 'Line Tariff Goods Value Amount'
            
            if esn_column not in df.columns or amount_column not in df.columns:
                self.logger.error(f"Required columns not found: {list(df.columns)}")
                return {}
            
            # Process efficiently
            cache = {}
            clean_df = df.dropna(subset=[esn_column, amount_column])
            
            for _, row in clean_df.iterrows():
                try:
                    esn = str(row[esn_column]).strip()
                    amount_str = str(row[amount_column]).strip()
                    clean_amount = amount_str.replace('$', '').replace(',', '').strip()
                    amount = float(clean_amount)
                    cache[esn] = amount
                except:
                    continue
            
            return cache
            
        except Exception as e:
            self.logger.error(f"Optimized method failed: {e}")
            raise
    
    async def _load_sheets_original(self) -> Dict[str, float]:
        """Fallback to original working method"""
        try:
            # Use the original method that was working
            cache = {}
            
            range_name = "Sheet1!A:Z"
            result = self.google_manager.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.google_manager.sheets_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return {}
            
            # Simple processing without pandas
            headers = values[0]
            data_rows = values[1:]
            
            # Find column indices
            esn_col_idx = None
            amount_col_idx = None
            
            for i, header in enumerate(headers):
                header_clean = str(header).strip().lower()
                if 'entry' in header_clean and 'summary' in header_clean and 'number' in header_clean:
                    esn_col_idx = i
                if 'line' in header_clean and 'tariff' in header_clean and 'amount' in header_clean:
                    amount_col_idx = i
            
            if esn_col_idx is None or amount_col_idx is None:
                return {}
            
            # Process rows
            for row in data_rows:
                try:
                    if len(row) > max(esn_col_idx, amount_col_idx):
                        esn = str(row[esn_col_idx]).strip()
                        amount_str = str(row[amount_col_idx]).strip()
                        clean_amount = amount_str.replace('$', '').replace(',', '').strip()
                        amount = float(clean_amount)
                        cache[esn] = amount
                except:
                    continue
            
            return cache
            
        except Exception as e:
            self.logger.error(f"Original method failed: {e}")
            raise
    
    async def _load_sheets_fallback(self) -> Dict[str, float]:
        """Emergency fallback method"""
        try:
            # Try with reduced data range
            range_name = "Sheet1!A1:Z500"  # Limit range
            result = self.google_manager.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.google_manager.sheets_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values or len(values) < 2:
                return {}
            
            # Very basic processing
            cache = {}
            for i, row in enumerate(values[1:], 1):  # Skip header
                try:
                    if len(row) >= 26:  # Ensure we have enough columns
                        esn = str(row[0]).strip()  # First column should be ESN
                        amount_str = str(row[25]).strip()  # Last column should be amount
                        
                        if esn.startswith('AE') and len(esn) >= 11:
                            clean_amount = amount_str.replace('$', '').replace(',', '').strip()
                            amount = float(clean_amount)
                            cache[esn] = amount
                except:
                    continue
            
            return cache
            
        except Exception as e:
            self.logger.error(f"Fallback method failed: {e}")
            raise
    
    async def get_esn_declared_amount(self, esn: str) -> Optional[float]:
        """Get ESN amount with fallback handling"""
        cache = await self.get_sheets_cache_with_retry()
        
        amount = cache.get(esn)
        if amount:
            self.logger.info(f"✅ Found {esn}: ${amount:,.2f}")
        else:
            self.logger.warning(f"❌ ESN {esn} not found in cache of {len(cache)} ESNs")
            # Show some sample ESNs for debugging
            sample_esns = list(cache.keys())[:5]
            self.logger.info(f"Sample cached ESNs: {sample_esns}")
        
        return amount
    
    def get_esn_folders_cached(self) -> List[Dict]:
        """Get ESN folders with caching"""
        if self._esn_folders_cache is None:
            self._esn_folders_cache = self.google_manager.get_all_esn_folders()
        return self._esn_folders_cache

class ProductionESNTester:
    """Production ESN tester with comprehensive result saving"""
    
    def __init__(self):
        self.config = SystemConfig()
        
        # Initialize services with error handling
        try:
            google_manager = GoogleServicesManager(
                self.config.GOOGLE_CREDENTIALS_PATH,
                self.config.GOOGLE_SHEETS_ID
            )
            self.cached_manager = RobustCachedGoogleManager(google_manager)
            self.invoice_processor = InvoiceProcessor(self.config)
            self.result_saver = ResultSaver(self.config)  # NEW: Result saving
            
            # Setup logging
            self._setup_logging()
            self.logger = logging.getLogger(__name__)
            
        except Exception as e:
            print(f"❌ Failed to initialize services: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        Path("logs").mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler()
            ]
        )
    
    async def test_specific_esn(self, target_esn: str) -> Optional[Dict]:
        """Test specific ESN with concurrent processing"""
        
        print("🚀 PRODUCTION ESN COMPLIANCE TEST")
        print("=" * 60)
        
        try:
            # Step 1: Load data with retries
            print("📊 Loading system data...")
            
            sheets_cache = await self.cached_manager.get_sheets_cache_with_retry()
            esn_folders = self.cached_manager.get_esn_folders_cached()
            
            # Step 2: Validate ESN exists
            declared_amount = sheets_cache.get(target_esn)
            esn_folder = next((info for info in esn_folders if info['esn'] == target_esn), None)
            
            if not declared_amount:
                print(f"❌ ESN {target_esn} not found in Google Sheets")
                print(f"📊 Available ESNs in sheets: {len(sheets_cache)}")
                if sheets_cache:
                    similar_esns = [esn for esn in sheets_cache.keys() if target_esn[:8] in esn]
                    if similar_esns:
                        print(f"🔍 Similar ESNs found: {similar_esns[:3]}")
                return None
            
            if not esn_folder:
                print(f"❌ ESN {target_esn} not found in Google Drive")
                print(f"📁 Available ESNs in drive: {len(esn_folders)}")
                return None
            
            print(f"✅ ESN validated: {target_esn}")
            print(f"💰 Declared Amount: ${declared_amount:,.2f}")
            
            # Step 3: Get invoice files
            print("\n📁 Finding invoice files...")
            invoice_files = self.cached_manager.google_manager.get_commercial_invoices_files(esn_folder['folder_id'])
            
            if not invoice_files:
                print("❌ No invoice files found")
                return None
            
            print(f"✅ Found {len(invoice_files)} PDF files")
            for file_info in invoice_files:
                size_mb = int(file_info.get('size', 0)) / (1024*1024) if file_info.get('size') else 0
                print(f"   📄 {file_info['name']} ({size_mb:.1f}MB)")
            
            # Step 4: Download files
            temp_dir = Path(self.config.TEMP_DIR) / target_esn
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            downloaded_files = []
            print("\n📥 Downloading files...")
            
            for i, file_info in enumerate(invoice_files, 1):
                print(f"   Downloading {i}/{len(invoice_files)}: {file_info['name']}")
                local_path = temp_dir / file_info['name']
                
                try:
                    if self.cached_manager.google_manager.download_file(file_info['id'], str(local_path)):
                        downloaded_files.append(str(local_path))
                        print(f"   ✅ Success")
                    else:
                        print(f"   ❌ Failed")
                except Exception as e:
                    print(f"   ❌ Error: {e}")
            
            if not downloaded_files:
                print("❌ No files downloaded successfully")
                return None
            
            print(f"✅ Downloaded {len(downloaded_files)} files")
            
            # Step 5: CONCURRENT AI Processing (REPLACE THIS SECTION)
            print(f"\n🤖 AI Processing {len(downloaded_files)} PDFs...")
            print("⚡ Using concurrent processing for maximum speed")
            
            ai_start = time.time()
            
            # Create semaphore for controlled concurrency
            max_concurrent = min(5, len(downloaded_files))  # Process up to 5 PDFs at once
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_single_pdf_concurrent(pdf_path: str, index: int):
                """Process single PDF with concurrency control"""
                async with semaphore:
                    pdf_name = Path(pdf_path).name
                    print(f"   🔄 Processing {index}/{len(downloaded_files)}: {pdf_name}")
                    
                    pdf_start = time.time()
                    try:
                        # Small delay to prevent API rate limiting
                        await asyncio.sleep(0.2 * index)  # Stagger requests
                        
                        invoice_data = await self.invoice_processor.process_single_invoice(pdf_path, target_esn)
                        pdf_duration = time.time() - pdf_start
                        
                        # Status icons
                        confidence_icons = {
                            "HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🟠", "ERROR": "🔴"
                        }
                        icon = confidence_icons.get(invoice_data.confidence_level.value, "❓")
                        
                        print(f"   {icon} ${invoice_data.total_usd_amount:,.2f} "
                              f"({invoice_data.confidence_level.value}, {pdf_duration:.1f}s)")
                        
                        return invoice_data
                        
                    except Exception as e:
                        pdf_duration = time.time() - pdf_start
                        print(f"   ❌ Error processing {pdf_name} ({pdf_duration:.1f}s): {e}")
                        
                        # Return error result instead of None
                        from models import CommercialInvoiceData, ConfidenceLevel
                        from decimal import Decimal
                        return CommercialInvoiceData(
                            invoice_number=f"ERROR_{Path(pdf_path).stem}",
                            company_name="ERROR",
                            total_usd_amount=Decimal('0'),
                            confidence_level=ConfidenceLevel.ERROR,
                            extraction_notes=f"Error: {str(e)[:100]}"
                        )
            
            # Run all PDFs concurrently
            print(f"   📊 Processing {len(downloaded_files)} PDFs with {max_concurrent} concurrent workers")
            
            tasks = [
                process_single_pdf_concurrent(pdf_path, i+1) 
                for i, pdf_path in enumerate(downloaded_files)
            ]
            
            # Execute all tasks concurrently
            extracted_invoices = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions and None results
            valid_invoices = []
            for result in extracted_invoices:
                if isinstance(result, Exception):
                    print(f"   ⚠️  Exception occurred: {result}")
                elif result is not None:
                    valid_invoices.append(result)
            
            ai_duration = time.time() - ai_start
            print(f"\n⚡ Concurrent processing completed in {ai_duration:.1f}s")
            print(f"   📊 Speed improvement: ~{max(1, 208.6/ai_duration):.1f}x faster")
            print(f"   ✅ Processed: {len(valid_invoices)}/{len(downloaded_files)} PDFs")
            
            # Use valid_invoices instead of extracted_invoices for the rest of the function
            extracted_invoices = valid_invoices
            
            # Step 6: Calculate results
            successful_invoices = [inv for inv in extracted_invoices if inv.confidence_level.value != "ERROR"]
            total_calculated = sum(inv.total_usd_amount for inv in successful_invoices)
            
            difference = abs(float(declared_amount) - float(total_calculated))
            percentage_diff = (difference / float(declared_amount) * 100) if declared_amount > 0 else 0
            is_compliant = percentage_diff <= self.config.TOLERANCE_PERCENTAGE
            
            # Step 7: Prepare comprehensive result data
            result_data = {
                "esn": target_esn,
                "declared_amount": float(declared_amount),
                "calculated_amount": float(total_calculated),
                "difference": difference,
                "percentage_difference": percentage_diff,
                "is_compliant": is_compliant,
                "successful_extractions": len(successful_invoices),
                "failed_extractions": len(extracted_invoices) - len(successful_invoices),
                "total_invoices": len(extracted_invoices),
                "ai_processing_time": ai_duration,
                "test_timestamp": datetime.now().isoformat(),
                "invoice_details": [
                    {
                        "invoice_number": inv.invoice_number,
                        "company_name": inv.company_name,
                        "amount": float(inv.total_usd_amount),
                        "client_reference": getattr(inv, 'client_reference', 'Not extracted'),
                        "material_description": getattr(inv, 'material_description', 'Not extracted'),
                        # ADD THESE NEW FIELDS:
                        "fecha_hora": getattr(inv, 'fecha_hora', 'Not extracted'),
                        "cantidad_total": getattr(inv, 'cantidad_total', 'Not extracted'),
                        "valor_unitario": getattr(inv, 'valor_unitario', 'Not extracted'),
                        "confidence": inv.confidence_level.value,
                        "currency": inv.currency,
                        "notes": inv.extraction_notes
                    }
                    for inv in extracted_invoices
                ]
            }
            
            # Step 8: SAVE RESULTS TO FILES
            print("\n💾 Saving test results...")
            saved_files = self.result_saver.save_test_result(result_data, "single_esn_test")
            
            # Step 9: Display results
            print("\n" + "=" * 60)
            print("📊 TEST RESULTS")
            print("=" * 60)
            print(f"🎯 ESN: {target_esn}")
            print(f"💰 Declared: ${declared_amount:,.2f}")
            print(f"💰 Calculated: ${total_calculated:,.2f}")
            print(f"📏 Difference: ${difference:,.2f}")
            
            print(f"\n📋 DETAILED INVOICE RESULTS:")
            for i, inv in enumerate(extracted_invoices, 1):
                print(f"   📄 Invoice {i}: {inv.company_name}")
                print(f"      💰 Amount: ${inv.total_usd_amount}")
                print(f"      🔍 SKU: {getattr(inv, 'client_reference', 'Not extracted')}")        # CHANGED: Reference → SKU
                print(f"      📝 Description: {getattr(inv, 'material_description', 'Not extracted')}")
                print(f"      📅 Date/Time: {getattr(inv, 'fecha_hora', 'Not extracted')}")
                print(f"      📦 Quantity: {getattr(inv, 'cantidad_total', 'Not extracted')}")
                print(f"      💵 Unit Value: {getattr(inv, 'valor_unitario', 'Not extracted')}")
                print(f"      ⭐ Confidence: {inv.confidence_level.value}")
                print(f"      ---")
            
            status_icon = "✅" if is_compliant else "❌"
            status_text = "COMPLIANT" if is_compliant else "NON-COMPLIANT"
            print(f"\n{status_icon} STATUS: {status_text}")
            
            print(f"\n📈 PROCESSING METRICS:")
            print(f"   Total PDFs: {len(extracted_invoices)}")
            print(f"   ✅ Successful: {len(successful_invoices)}")
            print(f"   ❌ Failed: {len(extracted_invoices) - len(successful_invoices)}")
            print(f"   ⏱️  AI Time: {ai_duration:.1f}s")
            print(f"   📊 Avg per PDF: {ai_duration/len(extracted_invoices):.1f}s")
            
            # Step 10: Display saved file locations
            if saved_files:
                print(f"\n💾 RESULTS SAVED TO:")
                for file_type, file_path in saved_files.items():
                    print(f"   📄 {file_type.upper()}: {file_path}")
            
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
            
            return result_data
            
        except Exception as e:
            self.logger.error(f"Test failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def run_quick_test(self) -> Optional[Dict]:
        """Run quick test with automatic ESN selection"""
        
        print("🚀 QUICK TEST WITH OPTIMAL ESN")
        print("=" * 60)
        
        try:
            # Load data
            sheets_cache = await self.cached_manager.get_sheets_cache_with_retry()
            esn_folders = self.cached_manager.get_esn_folders_cached()
            
            if not sheets_cache:
                print("❌ No data available in Google Sheets")
                return None
            
            if not esn_folders:
                print("❌ No ESN folders found in Google Drive")
                return None
            
            # Find matching ESNs
            cached_esns = set(sheets_cache.keys())
            drive_esns = set(info['esn'] for info in esn_folders)
            matching_esns = cached_esns.intersection(drive_esns)
            
            if not matching_esns:
                print("❌ No ESNs found in both systems")
                print(f"📊 Sheets ESNs: {len(cached_esns)}")
                print(f"📁 Drive ESNs: {len(drive_esns)}")
                return None
            
            # Pick first matching ESN for testing
            test_esn = list(matching_esns)[0]
            print(f"🎯 Auto-selected ESN: {test_esn}")
            
            return await self.test_specific_esn(test_esn)
            
        except Exception as e:
            print(f"❌ Quick test failed: {e}")
            return None
    
    async def run_diagnostics(self):
        """Run system diagnostics"""
        
        print("🔍 SYSTEM DIAGNOSTICS")
        print("=" * 50)
        
        # Test Google Sheets
        try:
            print("📊 Testing Google Sheets connection...")
            cache = await self.cached_manager.get_sheets_cache_with_retry()
            print(f"✅ Google Sheets: {len(cache)} ESNs loaded")
        except Exception as e:
            print(f"❌ Google Sheets: {e}")
        
        # Test Google Drive
        try:
            print("\n📁 Testing Google Drive connection...")
            folders = self.cached_manager.get_esn_folders_cached()
            print(f"✅ Google Drive: {len(folders)} ESN folders found")
        except Exception as e:
            print(f"❌ Google Drive: {e}")
        
        # Test output directory
        try:
            print("\n💾 Testing result saving...")
            print(f"✅ Output directory: {self.config.OUTPUT_DIR}")
            print(f"   📁 Tests folder: {Path(self.config.OUTPUT_DIR) / 'tests'}")
            print(f"   📁 Excel folder: {Path(self.config.OUTPUT_DIR) / 'excel'}")
            print(f"   📁 Detailed folder: {Path(self.config.OUTPUT_DIR) / 'detailed'}")
        except Exception as e:
            print(f"❌ Result saving setup: {e}")
        
        # Test data alignment
        try:
            print("\n🔄 Testing data alignment...")
            sheets_cache = await self.cached_manager.get_sheets_cache_with_retry()
            esn_folders = self.cached_manager.get_esn_folders_cached()
            
            if sheets_cache and esn_folders:
                cached_esns = set(sheets_cache.keys())
                drive_esns = set(info['esn'] for info in esn_folders)
                matching_esns = cached_esns.intersection(drive_esns)
                
                print(f"✅ Data Alignment:")
                print(f"   📊 Sheets ESNs: {len(cached_esns)}")
                print(f"   📁 Drive ESNs: {len(drive_esns)}")
                print(f"   🎯 Matching ESNs: {len(matching_esns)}")
                
                if matching_esns:
                    print(f"   📋 Sample matches: {list(matching_esns)[:3]}")
        except Exception as e:
            print(f"❌ Data alignment test: {e}")
        
        print("\n✅ Diagnostics completed")

# ============================================
# MAIN EXECUTION
# ============================================

async def main():
    """Main execution with comprehensive result saving"""
    
    print("🚀 PRODUCTION ESN COMPLIANCE TESTER")
    print("=" * 60)
    
    try:
        tester = ProductionESNTester()
        
        print("Choose testing mode:")
        print("1. Quick test with optimal ESN")
        print("2. Test specific ESN")
        print("3. Performance benchmark")
        print("4. System diagnostics")
        
        choice = input("\nEnter choice (1-4): ").strip()
        
        if choice == "1":
            result = await tester.run_quick_test()
            if result:
                print(f"\n🎉 QUICK TEST COMPLETED!")
                print(f"   Status: {'COMPLIANT' if result['is_compliant'] else 'NON-COMPLIANT'}")
                print(f"   Accuracy: {result['percentage_difference']:.2f}% difference")
                print(f"   Results saved to: {tester.config.OUTPUT_DIR}")
        
        elif choice == "2":
            target_esn = input("Enter ESN to test (e.g., AE900683929): ").strip()
            if target_esn:
                result = await tester.test_specific_esn(target_esn)
                if result:
                    print(f"\n🎉 TEST COMPLETED!")
                    print(f"   Status: {'COMPLIANT' if result['is_compliant'] else 'NON-COMPLIANT'}")
                    print(f"   Results saved to: {tester.config.OUTPUT_DIR}")
            else:
                print("❌ No ESN provided")
        
        elif choice == "3":
            print("🏁 Performance benchmark not implemented yet")
            print("   Use option 1 or 2 for testing")
        
        elif choice == "4":
            await tester.run_diagnostics()
        
        else:
            print("❌ Invalid choice")
    
    except Exception as e:
        print(f"❌ System error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())