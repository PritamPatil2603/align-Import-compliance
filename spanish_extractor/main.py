import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .config import SpanishExtractorConfig
from .google_drive_service import SpanishDriveService
from .extractor import SpanishInvoiceExtractor
from .exporters import SpanishInvoiceExporter
from .models import ExtractionSummary, ESNData, ExtractionConfidence, InvoiceData

# Setup logging
def setup_logging(config: SpanishExtractorConfig):
    """Setup comprehensive logging"""
    
    # Create logs directory
    log_dir = Path(config.LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"spanish_extractor_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Set library log levels
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Spanish Invoice Extractor started - Log file: {log_file}")
    
    return logger

class SpanishInvoiceProcessor:
    """Main Spanish invoice processor"""
    
    def __init__(self, root_folder_id: str):
        self.config = SpanishExtractorConfig()
        self.root_folder_id = root_folder_id
        self.logger = setup_logging(self.config)
        
        # Initialize services
        self.drive_service = SpanishDriveService(self.config.GOOGLE_CREDENTIALS_PATH)
        self.extractor = SpanishInvoiceExtractor(self.config)
        self.exporter = SpanishInvoiceExporter(self.config.EXPORT_DIR)
        
        # Processing stats
        self.start_time = time.time()
        self.processing_stats = {
            'esns_found': 0,
            'esns_processed': 0,
            'total_invoices': 0,
            'successful_extractions': 0,
            'failed_extractions': 0
        }
        
        self.logger.info("✅ Spanish Invoice Processor initialized")
    
    async def process_all_esns(self) -> ExtractionSummary:
        """Process all ESN folders and extract invoice data"""
        
        self.logger.info(f"🔍 Starting extraction from root folder: {self.root_folder_id}")
        
        try:
            # Step 1: Discover all ESN folders
            esn_folders = self._discover_esn_folders()
            
            if not esn_folders:
                self.logger.error("❌ No ESN folders found")
                return self._create_empty_summary("No ESN folders found")
            
            self.processing_stats['esns_found'] = len(esn_folders)
            self.logger.info(f"✅ Found {len(esn_folders)} ESN folders")
            
            # Step 2: Process each ESN
            extraction_summary = await self._process_all_esns(esn_folders)
            
            # Step 3: Finalize summary
            self._finalize_summary(extraction_summary)
            
            # Step 4: Export results
            await self._export_results(extraction_summary)
            
            return extraction_summary
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            return self._create_empty_summary(f"Processing error: {e}")
    
    def _discover_esn_folders(self) -> List[Dict[str, str]]:
        """Discover all ESN folders in the root directory"""
        
        try:
            self.logger.info(f"🔍 Scanning root folder for ESN directories...")
            
            esn_folders = self.drive_service.get_esn_folders(self.root_folder_id)
            
            if esn_folders:
                self.logger.info(f"📁 ESN folders found:")
                for folder in esn_folders:
                    self.logger.info(f"   - {folder['name']} (ID: {folder['id']})")
            
            return esn_folders
            
        except Exception as e:
            self.logger.error(f"❌ Error discovering ESN folders: {e}")
            return []
    
    async def _process_all_esns(self, esn_folders: List[Dict[str, str]]) -> ExtractionSummary:
        """Process all ESN folders"""
        
        # Initialize extraction summary
        extraction_summary = ExtractionSummary(
            timestamp=datetime.now().isoformat(),
            total_esns=len(esn_folders),
            total_invoices=0,
            total_line_items=0,
            processing_time_seconds=0.0,
            success_rate_percentage=0.0
        )
        
        # Process each ESN
        for esn_folder in esn_folders:
            esn_name = esn_folder['name']
            
            try:
                self.logger.info(f"\n📁 Processing ESN: {esn_name}")
                
                # Process this ESN
                esn_data = await self._process_single_esn(esn_folder)
                
                if esn_data:
                    extraction_summary.esn_data[esn_name] = esn_data
                    self.processing_stats['esns_processed'] += 1
                    
                    # Log ESN results
                    success_rate = esn_data.get_success_rate()
                    self.logger.info(f"✅ {esn_name}: {esn_data.total_invoices} invoices, "
                                   f"{esn_data.total_line_items} items, "
                                   f"${esn_data.total_declared_value_usd:,.2f}, "
                                   f"{success_rate:.1f}% success")
                
            except Exception as e:
                self.logger.error(f"❌ Error processing ESN {esn_name}: {e}")
                
                # Create error ESN data
                error_esn = ESNData(
                    esn=esn_name,
                    total_invoices=0,
                    total_line_items=0,
                    total_declared_value_usd=0,
                    processing_status="ERROR",
                    invoices={}
                )
                extraction_summary.esn_data[esn_name] = error_esn
        
        return extraction_summary
    
    async def _process_single_esn(self, esn_folder: Dict[str, str]) -> ESNData:
        """Process a single ESN folder"""
        
        esn_name = esn_folder['name']
        esn_id = esn_folder['id']
        
        # Step 1: Find commercial invoices folder
        commercial_folder_id = self.drive_service.get_commercial_invoices_folder(esn_id)
        
        if not commercial_folder_id:
            self.logger.warning(f"⚠️ No COMMERCIAL INVOICES folder found in {esn_name}")
            return ESNData(
                esn=esn_name,
                total_invoices=0,
                total_line_items=0,
                total_declared_value_usd=0,
                processing_status="NO_COMMERCIAL_INVOICES",
                invoices={}
            )
        
        # Step 2: Get all PDF files
        pdf_files = self.drive_service.get_pdf_files(commercial_folder_id)
        
        if not pdf_files:
            self.logger.warning(f"⚠️ No PDF files found in COMMERCIAL INVOICES for {esn_name}")
            return ESNData(
                esn=esn_name,
                total_invoices=0,
                total_line_items=0,
                total_declared_value_usd=0,
                processing_status="NO_PDFS",
                invoices={}
            )
        
        self.logger.info(f"📄 Found {len(pdf_files)} PDF files in {esn_name}")
        self.processing_stats['total_invoices'] += len(pdf_files)
        
        # Step 3: Process all PDFs with concurrency control
        invoice_results = await self._process_esn_pdfs(esn_name, pdf_files)
        
        # Step 4: Create ESN data
        esn_data = ESNData(
            esn=esn_name,
            total_invoices=len(invoice_results),
            total_line_items=0,
            total_declared_value_usd=0,
            processing_status="SUCCESS" if invoice_results else "FAILED",
            invoices=invoice_results
        )
        
        # Calculate totals
        esn_data.calculate_totals()
        
        return esn_data
    
    async def _process_esn_pdfs(self, esn_name: str, pdf_files: List[Dict[str, str]]) -> Dict[str, 'InvoiceData']:
        """Process all PDFs for one ESN with concurrency control"""
        
        results = {}
        temp_dir = Path(self.config.TEMP_DIR) / esn_name
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Control concurrency to avoid overwhelming the system
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_PDFS)
        
        async def process_single_pdf(file_info: Dict[str, str]):
            """Process a single PDF file"""
            async with semaphore:
                file_name = file_info['name']
                file_id = file_info['id']
                
                try:
                    # Download PDF
                    local_path = temp_dir / file_name
                    
                    if self.drive_service.download_file(file_id, str(local_path)):
                        # Extract data
                        self.logger.debug(f"🔄 Extracting data from {file_name}")
                        invoice_data = await self.extractor.extract_from_pdf(str(local_path), esn_name)
                        
                        # Cleanup
                        local_path.unlink(missing_ok=True)
                        
                        # Update stats
                        if invoice_data.extraction_confidence != ExtractionConfidence.ERROR:
                            self.processing_stats['successful_extractions'] += 1
                        else:
                            self.processing_stats['failed_extractions'] += 1
                        
                        return file_name, invoice_data
                    else:
                        self.logger.error(f"❌ Failed to download {file_name}")
                        return None
                        
                except Exception as e:
                    self.logger.error(f"❌ Error processing {file_name}: {e}")
                    return None
        
        # Process all PDFs concurrently
        self.logger.info(f"🔄 Processing {len(pdf_files)} PDFs with max {self.config.MAX_CONCURRENT_PDFS} concurrent...")
        
        tasks = [process_single_pdf(file_info) for file_info in pdf_files]
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter valid results
        for result in results_raw:
            if result and not isinstance(result, Exception):
                file_name, invoice_data = result
                results[file_name] = invoice_data
        
        # Cleanup temp directory
        try:
            temp_dir.rmdir()
        except:
            pass  # Directory not empty or other cleanup issues
        
        return results
    
    def _finalize_summary(self, extraction_summary: ExtractionSummary):
        """Finalize extraction summary with calculated stats"""
        
        # Calculate processing time
        extraction_summary.processing_time_seconds = time.time() - self.start_time
        
        # Calculate summary statistics
        extraction_summary.calculate_summary_stats()
        
        # Update processing stats
        self.processing_stats.update({
            'total_processing_time': extraction_summary.processing_time_seconds,
            'success_rate': extraction_summary.success_rate_percentage
        })
        
        # Log final statistics
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 FINAL EXTRACTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"📁 ESNs: {self.processing_stats['esns_processed']}/{self.processing_stats['esns_found']}")
        self.logger.info(f"📄 PDFs: {self.processing_stats['successful_extractions']}/{self.processing_stats['total_invoices']}")
        self.logger.info(f"📦 Line Items: {extraction_summary.total_line_items}")
        self.logger.info(f"💰 Total Value: ${extraction_summary.get_total_declared_value():,.2f}")
        self.logger.info(f"⏱️ Processing Time: {extraction_summary.processing_time_seconds:.1f}s")
        self.logger.info(f"✅ Success Rate: {extraction_summary.success_rate_percentage:.1f}%")
        
        # Log extractor stats
        extractor_stats = self.extractor.get_stats()
        self.logger.info(f"🤖 Extractor Stats:")
        self.logger.info(f"   - AI Attempts: {extractor_stats.get('ai_extraction_attempts', 0)}")
        self.logger.info(f"   - Regex Fallbacks: {extractor_stats.get('regex_fallback_uses', 0)}")
        self.logger.info(f"   - Avg Line Items: {extractor_stats.get('avg_line_items_per_invoice', 0):.1f}")
    
    async def _export_results(self, extraction_summary: ExtractionSummary):
        """Export results to all formats"""
        
        try:
            self.logger.info("\n📤 Exporting results...")
            
            # Export to all formats
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_paths = self.exporter.export_all_formats(extraction_summary)
            
            # Export summary report
            summary_report = self.exporter.export_summary_report(extraction_summary, timestamp)
            if summary_report:
                export_paths['summary_report'] = summary_report
            
            # Log export results
            self.logger.info("✅ Export completed:")
            for format_type, path in export_paths.items():
                if path:
                    self.logger.info(f"   📊 {format_type.upper()}: {path}")
                else:
                    self.logger.warning(f"   ❌ {format_type.upper()}: Export failed")
            
        except Exception as e:
            self.logger.error(f"❌ Export error: {e}")
    
    def _create_empty_summary(self, error_message: str) -> ExtractionSummary:
        """Create empty summary for error cases"""
        return ExtractionSummary(
            timestamp=datetime.now().isoformat(),
            total_esns=0,
            total_invoices=0,
            total_line_items=0,
            processing_time_seconds=time.time() - self.start_time,
            success_rate_percentage=0.0,
            esn_data={}
        )

# Main function for direct execution
async def main():
    """Main function for command-line execution"""
    
    print("🇪🇸 SPANISH INVOICE DATA EXTRACTOR")
    print("=" * 50)
    print("Extracts line items from Spanish commercial invoices")
    print("for US import compliance auditing.\n")
    
    # Get root folder ID from user
    root_folder_id = input("Enter Google Drive ROOT folder ID: ").strip()
    
    if not root_folder_id:
        print("❌ Root folder ID is required")
        return
    
    print(f"\n🚀 Starting extraction from folder: {root_folder_id}")
    print("This may take several minutes depending on the number of PDFs...\n")
    
    try:
        # Initialize processor
        processor = SpanishInvoiceProcessor(root_folder_id)
        
        # Process all ESNs
        results = await processor.process_all_esns()
        
        # Display results
        if results.total_esns > 0:
            print("\n🎉 EXTRACTION COMPLETED!")
            print(f"📁 ESNs processed: {results.total_esns}")
            print(f"📄 Invoices processed: {results.total_invoices}")
            print(f"📦 Line items extracted: {results.total_line_items}")
            print(f"💰 Total declared value: ${results.get_total_declared_value():,.2f}")
            print(f"✅ Success rate: {results.success_rate_percentage:.1f}%")
            print(f"⏱️ Processing time: {results.processing_time_seconds:.1f} seconds")
            print(f"\n📊 Check the 'data/spanish_exports' folder for results!")
        else:
            print("❌ No data extracted. Check logs for details.")
            
    except KeyboardInterrupt:
        print("\n⚠️ Extraction cancelled by user")
    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")
        print("Check the log file for detailed error information.")

if __name__ == "__main__":
    asyncio.run(main())