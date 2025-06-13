# ============================================
# FILE: src/spanish_invoice_extractor.py
# 🇪🇸 Spanish Invoice Extractor with LlamaParse
# Enhanced with batch processing, incremental saving, and resume capability
# ============================================

import asyncio
import logging
import time
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Leverage existing robust infrastructure
from config import SystemConfig
from google_services import GoogleServicesManager
from invoice_processor import OptimizedInvoiceProcessor
from models import CommercialInvoiceData, ConfidenceLevel, EnhancedInvoiceData, InvoiceExtractionResult
from export_manager import ExportManager

class SpanishInvoiceExtractor:
    """🇪🇸 Enhanced Spanish invoice data extraction with batch processing and resume capability"""
    
    def __init__(self):
        """Initialize with existing robust infrastructure"""
        self.config = SystemConfig()
        
        # Leverage existing services (keep backward compatibility)
        self.google_manager = GoogleServicesManager(
            self.config.GOOGLE_CREDENTIALS_PATH,
            self.config.GOOGLE_SHEETS_ID
        )
        
        # Use the enhanced invoice processor (already working perfectly)
        self.invoice_processor = OptimizedInvoiceProcessor(self.config)
        
        # New export manager for specialized formats
        self.export_manager = ExportManager(self.config)
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Statistics tracking
        self.stats = {
            'total_esn_folders': 0,
            'total_pdfs_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_line_items': 0,
            'processing_start_time': None,
            'processing_end_time': None
        }
    
    def _setup_logging(self):
        """Setup logging for extractor"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        Path("logs").mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/spanish_extractor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler()
            ]
        )
    
    async def extract_with_batch_processing(self, batch_size: int = 20, resume_session: str = None) -> Dict[str, Any]:
        """🚀 NEW: Extract with batch processing and resume capability"""
        
        print("🇪🇸 SPANISH INVOICE EXTRACTOR - BATCH PROCESSING MODE")
        print("=" * 70)
        
        # Check for resumable sessions
        if not resume_session:
            resumable = self.export_manager.find_resumable_sessions()
            if resumable:
                print(f"\n🔄 Found {len(resumable)} resumable sessions:")
                for i, session in enumerate(resumable, 1):
                    print(f"   {i}. Session {session['session_id']}")
                    print(f"      Progress: {session['completed_esns']}/{session['total_esns']} ESNs")
                    print(f"      Last updated: {session['last_updated']}")
                
                choice = input("\nResume existing session? Enter number (or 'n' for new): ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(resumable):
                    resume_session = resumable[int(choice) - 1]['session_id']
        
        # Initialize or resume session
        if resume_session:
            print(f"🔄 Resuming session: {resume_session}")
            exporter = self.export_manager.create_incremental_exporter(resume_session)
            completed_esns = set(exporter.get_completed_esns())
            failed_esns = set(exporter.get_failed_esns())
            print(f"📥 Loaded existing progress: {len(completed_esns)} completed, {len(failed_esns)} failed")
        else:
            print("🆕 Starting new batch processing session")
            exporter = self.export_manager.create_incremental_exporter()
            completed_esns = set()
            failed_esns = set()
        
        self.stats['processing_start_time'] = time.time()
        
        try:
            # Get all ESN folders
            print("📁 Scanning Google Drive for ESN folders...")
            all_esn_folders = self.google_manager.get_all_esn_folders()
            
            # Filter out already completed/failed ESNs
            remaining_esns = [
                esn_info for esn_info in all_esn_folders 
                if esn_info['esn'] not in completed_esns and esn_info['esn'] not in failed_esns
            ]
            
            if not remaining_esns:
                print("✅ All ESNs already processed!")
                exporter.finalize_session()
                return exporter.get_final_results()
            
            print(f"🎯 Found {len(remaining_esns)} ESNs to process")
            if completed_esns:
                print(f"📋 Already completed: {len(completed_esns)} ESNs")
            if failed_esns:
                print(f"❌ Previously failed: {len(failed_esns)} ESNs")
            
            # Set total for progress tracking
            exporter.initialize_processing(len(all_esn_folders), [info['esn'] for info in all_esn_folders])
            
            # Process in batches
            total_batches = (len(remaining_esns) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(remaining_esns))
                batch_esns = remaining_esns[start_idx:end_idx]
                
                print(f"\n🔄 Processing Batch {batch_num + 1}/{total_batches}")
                print(f"📊 ESNs {start_idx + 1}-{end_idx} of {len(remaining_esns)} remaining")
                
                # Process each ESN in the batch
                for i, esn_info in enumerate(batch_esns, 1):
                    esn = esn_info['esn']
                    folder_id = esn_info['folder_id']
                    
                    print(f"\n📂 [{start_idx + i}/{len(remaining_esns)}] Processing ESN: {esn}")
                    
                    try:
                        start_time = time.time()
                        esn_data = await self._extract_single_esn_folder(esn, folder_id)
                        processing_time = time.time() - start_time
                        
                        if esn_data:
                            exporter.add_esn_data(esn, esn_data, processing_time)
                            print(f"   ✅ Completed: {len(esn_data)} invoices extracted in {processing_time:.1f}s")
                        else:
                            exporter.add_failed_esn(esn, "No data extracted")
                            print(f"   ❌ No data extracted")
                    
                    except Exception as e:
                        exporter.add_failed_esn(esn, str(e))
                        print(f"   ❌ Error: {str(e)}")
                        self.logger.error(f"Failed to process ESN {esn}: {e}")
                
                # Show batch completion and remaining time estimate
                completed_so_far = len(exporter.get_completed_esns())
                if completed_so_far > 0:
                    avg_time_per_esn = exporter.get_average_processing_time()
                    remaining_count = len(remaining_esns) - end_idx
                    estimated_remaining = remaining_count * avg_time_per_esn
                    
                    print(f"✅ Batch {batch_num + 1} completed")
                    print(f"⏱️ Estimated remaining time: {estimated_remaining/60:.1f} minutes")
                    print(f"📊 Progress: {end_idx}/{len(remaining_esns)} remaining ESNs")
            
            # Finalize session
            self.stats['processing_end_time'] = time.time()
            exporter.finalize_session()
            
            final_results = exporter.get_final_results()
            
            print(f"\n🎉 BATCH PROCESSING COMPLETED!")
            print(f"📊 Final Statistics:")
            print(f"   ✅ Successful: {len(exporter.get_completed_esns())}")
            print(f"   ❌ Failed: {len(exporter.get_failed_esns())}")
            print(f"   📄 Total Invoices: {exporter.get_total_invoices()}")
            print(f"   📦 Total Line Items: {exporter.get_total_line_items()}")
            print(f"   ⏱️ Total Time: {(self.stats['processing_end_time'] - self.stats['processing_start_time'])/60:.1f} minutes")
            
            return final_results
            
        except KeyboardInterrupt:
            print("\n⚠️ Processing interrupted by user")
            print("💾 Progress has been saved automatically!")
            print(f"🔄 Resume with session ID: {exporter.session_id}")
            exporter.save_interruption_state()
            return exporter.get_final_results()
        
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            exporter.add_processing_error(str(e))
            return exporter.get_final_results()
    
    async def extract_all_esn_folders(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """🔄 LEGACY: Extract data from ALL ESN folders (maintained for backward compatibility)"""
        
        print("🇪🇸 SPANISH INVOICE EXTRACTOR - PROCESSING ALL ESN FOLDERS")
        print("=" * 70)
        print("⚠️ Note: Using legacy mode. Consider using batch processing for better reliability.")
        
        self.stats['processing_start_time'] = time.time()
        
        try:
            # Step 1: Get all ESN folders from Google Drive
            print("📁 Scanning Google Drive for ESN folders...")
            esn_folders = self.google_manager.get_all_esn_folders()
            
            if not esn_folders:
                print("❌ No ESN folders found in Google Drive")
                return {}
            
            # Apply limit if specified
            if limit:
                esn_folders = esn_folders[:limit]
                print(f"🎯 Processing first {limit} ESN folders (limit applied)")
            
            self.stats['total_esn_folders'] = len(esn_folders)
            print(f"✅ Found {len(esn_folders)} ESN folders to process")
            
            # Step 2: Process each ESN folder
            all_extractions = []
            
            for i, esn_info in enumerate(esn_folders, 1):
                esn = esn_info['esn']
                folder_id = esn_info['folder_id']
                
                print(f"\n📂 [{i}/{len(esn_folders)}] Processing ESN: {esn}")
                
                # Extract data from this ESN folder
                esn_data = await self._extract_single_esn_folder(esn, folder_id)
                
                if esn_data:
                    all_extractions.extend(esn_data)
                    print(f"   ✅ Extracted {len(esn_data)} invoices from {esn}")
                else:
                    print(f"   ❌ No data extracted from {esn}")
            
            self.stats['processing_end_time'] = time.time()
            total_time = self.stats['processing_end_time'] - self.stats['processing_start_time']
            
            # Step 3: Create comprehensive results
            results = {
                'extraction_metadata': {
                    'extraction_date': datetime.now().isoformat(),
                    'total_esn_folders': len(esn_folders),
                    'total_invoices_extracted': len(all_extractions),
                    'successful_extractions': self.stats['successful_extractions'],
                    'failed_extractions': self.stats['failed_extractions'],
                    'total_line_items': self.stats['total_line_items'],
                    'processing_time_seconds': total_time,
                    'avg_time_per_esn': total_time / len(esn_folders) if esn_folders else 0,
                    'extraction_success_rate': (self.stats['successful_extractions'] / self.stats['total_pdfs_processed'] * 100) if self.stats['total_pdfs_processed'] > 0 else 0
                },
                'extracted_data': all_extractions
            }
            
            # Step 4: Export to multiple formats
            print(f"\n💾 Exporting {len(all_extractions)} invoice records to multiple formats...")
            export_paths = await self.export_manager.export_all_formats(results)
            
            # Step 5: Display summary
            self._display_extraction_summary(results, export_paths)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to extract all ESN folders: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    async def extract_specific_esn(self, target_esn: str) -> Dict[str, Any]:
        """Extract data from a specific ESN folder"""
        
        print(f"🇪🇸 SPANISH INVOICE EXTRACTOR - PROCESSING ESN: {target_esn}")
        print("=" * 70)
        
        try:
            # Find the specific ESN folder
            esn_folders = self.google_manager.get_all_esn_folders()
            esn_info = next((info for info in esn_folders if info['esn'] == target_esn), None)
            
            if not esn_info:
                print(f"❌ ESN {target_esn} not found in Google Drive")
                return {}
            
            print(f"✅ Found ESN folder: {target_esn}")
            
            # Extract data from this ESN
            esn_data = await self._extract_single_esn_folder(target_esn, esn_info['folder_id'])
            
            if not esn_data:
                print("❌ No data extracted")
                return {}
            
            # Create results
            results = {
                'extraction_metadata': {
                    'extraction_date': datetime.now().isoformat(),
                    'target_esn': target_esn,
                    'total_invoices_extracted': len(esn_data),
                    'total_line_items': sum(len(inv.get('line_items', [])) for inv in esn_data)
                },
                'extracted_data': esn_data
            }
            
            # Export to multiple formats
            print(f"\n💾 Exporting {len(esn_data)} invoice records...")
            export_paths = await self.export_manager.export_all_formats(results, f"single_esn_{target_esn}")
            
            # Display summary
            self._display_extraction_summary(results, export_paths)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to extract ESN {target_esn}: {e}")
            return {}
    
    async def _extract_single_esn_folder(self, esn: str, folder_id: str) -> List[Dict[str, Any]]:
        """Extract all invoice data from a single ESN folder"""
        
        try:
            # Step 1: Get all commercial invoice PDFs
            invoice_files = self.google_manager.get_commercial_invoices_files(folder_id)
            
            if not invoice_files:
                print(f"   📄 No PDF files found in {esn}")
                return []
            
            print(f"   📄 Found {len(invoice_files)} PDF files")
            
            # Step 2: Download files to temp directory
            temp_dir = Path(self.config.TEMP_DIR) / f"extraction_{esn}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            downloaded_files = []
            for file_info in invoice_files:
                local_path = temp_dir / file_info['name']
                if self.google_manager.download_file(file_info['id'], str(local_path)):
                    downloaded_files.append({
                        'path': str(local_path),
                        'name': file_info['name'],
                        'original_info': file_info
                    })
            
            if not downloaded_files:
                print(f"   ❌ No files downloaded for {esn}")
                return []
            
            print(f"   📥 Downloaded {len(downloaded_files)} files")
            
            # Step 3: Process all PDFs with enhanced extraction
            print(f"   🤖 Processing {len(downloaded_files)} PDFs with AI...")
            
            # Use concurrent processing for speed
            semaphore = asyncio.Semaphore(3)  # Control concurrency
            
            async def process_single_pdf(file_info: Dict) -> Optional[Dict[str, Any]]:
                async with semaphore:
                    try:
                        # Use enhanced invoice processor (already working!)
                        result = await self.invoice_processor.process_single_invoice_enhanced(
                            file_info['path'], esn
                        )
                        
                        # Convert to our target format
                        extracted_invoice = self._convert_to_target_format(
                            result, esn, file_info['name']
                        )
                        
                        if extracted_invoice:
                            self.stats['successful_extractions'] += 1
                            self.stats['total_line_items'] += len(extracted_invoice.get('line_items', []))
                        else:
                            self.stats['failed_extractions'] += 1
                        
                        return extracted_invoice
                        
                    except Exception as e:
                        self.logger.error(f"Error processing {file_info['name']}: {e}")
                        self.stats['failed_extractions'] += 1
                        return None
            
            # Process all PDFs concurrently
            tasks = [process_single_pdf(file_info) for file_info in downloaded_files]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter successful results
            extracted_invoices = []
            for result in results:
                if isinstance(result, dict) and result is not None:
                    extracted_invoices.append(result)
            
            self.stats['total_pdfs_processed'] += len(downloaded_files)
            
            print(f"   ✅ Successfully processed {len(extracted_invoices)}/{len(downloaded_files)} PDFs")
            
            # Cleanup temp files
            for file_info in downloaded_files:
                try:
                    Path(file_info['path']).unlink()
                except:
                    pass
            try:
                temp_dir.rmdir()
            except:
                pass
            
            return extracted_invoices
            
        except Exception as e:
            self.logger.error(f"Error extracting ESN folder {esn}: {e}")
            return []
    
    def _convert_to_target_format(self, result: InvoiceExtractionResult, esn: str, pdf_name: str) -> Optional[Dict[str, Any]]:
        """Convert enhanced extraction result to our target format with 7 key fields"""
        
        try:
            enhanced_data = result.enhanced_data
            
            # Base invoice data with 7 key fields
            invoice_data = {
                'esn': esn,
                'pdf_filename': pdf_name,
                'extraction_timestamp': datetime.now().isoformat(),
                
                # 🎯 7 KEY FIELDS from Spanish commercial invoices:
                'fecha_hora': enhanced_data.fecha_hora,  # Date & Time (Fecha y hora de emisión)
                'supplier': enhanced_data.company_name,  # Supplier (Emisor/Company name)
                'client_reference': enhanced_data.client_reference,  # Client Reference (REF CLIENTE/SKU)
                'material_description': enhanced_data.material_description,  # Material Description
                'cantidad_total': enhanced_data.cantidad_total,  # Total Units (CANTIDAD TOTAL)
                'valor_unitario': enhanced_data.valor_unitario,  # Unit Value (VALOR UNITARIO)  
                'total_usd_amount': float(enhanced_data.total_usd_amount),  # Total USD amount
                
                # Additional metadata
                'currency': enhanced_data.currency,
                'confidence_level': enhanced_data.confidence_level.value,
                'extraction_method': result.extraction_method,
                'processing_time_seconds': result.processing_time,
                'line_item_extraction_success': result.line_item_extraction_success,
                
                # Line items (if available)
                'line_items_count': len(enhanced_data.line_items),
                'line_items': []
            }
            
            # Add line items if available (enhanced format)
            if enhanced_data.line_items:
                for item in enhanced_data.line_items:
                    line_item = {
                        'line_number': item.line_number,
                        'sku': item.sku,  # Individual SKU per line
                        'description': item.description,  # Individual description per line
                        'quantity': float(item.quantity),  # Individual quantity per line
                        'unit_price': float(item.unit_price),  # Individual unit price per line
                        'line_total': float(item.line_total),  # Individual line total
                        'unit_of_measure': item.unit_of_measure,
                        'country_of_origin': item.country_of_origin,
                        'hts_code': item.hts_code
                    }
                    invoice_data['line_items'].append(line_item)
            
            return invoice_data
            
        except Exception as e:
            self.logger.error(f"Error converting result to target format: {e}")
            return None
    
    def _display_extraction_summary(self, results: Dict, export_paths: Dict[str, str]):
        """Display comprehensive extraction summary"""
        
        metadata = results['extraction_metadata']
        extracted_data = results['extracted_data']
        
        print("\n" + "=" * 70)
        print("📊 SPANISH INVOICE EXTRACTION SUMMARY")
        print("=" * 70)
        
        # Overall statistics
        print(f"🎯 Total ESN Folders: {metadata.get('total_esn_folders', 1)}")
        print(f"📄 Total Invoices Extracted: {len(extracted_data)}")
        print(f"✅ Successful Extractions: {metadata.get('successful_extractions', 0)}")
        print(f"❌ Failed Extractions: {metadata.get('failed_extractions', 0)}")
        print(f"📦 Total Line Items: {metadata.get('total_line_items', 0)}")
        print(f"⏱️  Processing Time: {metadata.get('processing_time_seconds', 0):.1f}s")
        print(f"📈 Success Rate: {metadata.get('extraction_success_rate', 0):.1f}%")
        
        # Sample of extracted data
        if extracted_data:
            print(f"\n📋 SAMPLE EXTRACTED DATA:")
            for i, invoice in enumerate(extracted_data[:3], 1):  # Show first 3
                print(f"   📄 Invoice {i}:")
                print(f"      🆔 ESN: {invoice['esn']}")
                print(f"      📁 PDF: {invoice['pdf_filename']}")
                print(f"      🏢 Supplier: {invoice['supplier']}")
                print(f"      💰 Amount: ${invoice['total_usd_amount']:,.2f}")
                print(f"      📅 Date: {invoice['fecha_hora']}")
                print(f"      📦 Line Items: {invoice['line_items_count']}")
                if invoice['line_items']:
                    print(f"         • First SKU: {invoice['line_items'][0]['sku']}")
                print(f"      ---")
            
            if len(extracted_data) > 3:
                print(f"   ... and {len(extracted_data) - 3} more invoices")
        
        # Export file locations
        print(f"\n💾 EXTRACTED DATA SAVED TO:")
        for format_type, file_path in export_paths.items():
            print(f"   📄 {format_type.upper()}: {file_path}")
        
        print("\n🎉 Spanish Invoice Extraction Completed!")

# ============================================
# MAIN EXECUTION
# ============================================

async def main():
    """Enhanced main execution with batch processing options"""
    
    print("🇪🇸 SPANISH INVOICE EXTRACTOR")
    print("=" * 50)
    
    try:
        extractor = SpanishInvoiceExtractor()
        
        print("Choose extraction mode:")
        print("1. 🚀 BATCH PROCESSING (RECOMMENDED - with resume capability)")
        print("2. 📂 Extract specific ESN folder")
        print("3. 🧪 Extract limited ESN folders (testing)")
        print("4. 🔄 Resume interrupted session")
        print("5. 🔧 Legacy: Extract ALL ESN folders (no batching)")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            batch_size = input("Enter batch size (default 20): ").strip()
            batch_size = int(batch_size) if batch_size.isdigit() else 20
            
            print(f"\n🚀 Starting batch processing (batch size: {batch_size})...")
            print("💾 Progress will be saved after each ESN!")
            print("🔄 You can safely interrupt and resume later.")
            results = await extractor.extract_with_batch_processing(batch_size=batch_size)
            
        elif choice == "2":
            esn = input("Enter ESN to extract (e.g., AE900683929): ").strip()
            if esn:
                results = await extractor.extract_specific_esn(esn)
            else:
                print("❌ No ESN provided")
                return
        
        elif choice == "3":
            limit = input("Enter number of ESN folders: ").strip()
            try:
                limit = int(limit)
                results = await extractor.extract_all_esn_folders(limit=limit)
            except ValueError:
                print("❌ Invalid number")
                return
        
        elif choice == "4":
            # Show resumable sessions
            resumable = extractor.export_manager.find_resumable_sessions()
            if not resumable:
                print("❌ No resumable sessions found")
                return
            
            print(f"📋 Found {len(resumable)} resumable sessions:")
            for i, session in enumerate(resumable, 1):
                print(f"   {i}. {session['session_id']} - {session['completed_esns']}/{session['total_esns']} ESNs")
                print(f"      Last updated: {session['last_updated']}")
            
            choice = input("Select session to resume (number): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(resumable):
                session_id = resumable[int(choice) - 1]['session_id']
                results = await extractor.extract_with_batch_processing(resume_session=session_id)
            else:
                print("❌ Invalid selection")
                return
        
        elif choice == "5":
            print("\n⚠️ Using legacy mode - no incremental saving!")
            confirm = input("Continue? (y/n): ").strip().lower()
            if confirm == 'y':
                results = await extractor.extract_all_esn_folders()
            else:
                return
        
        else:
            print("❌ Invalid choice")
            return
        
        if results and results.get('extracted_data'):
            print(f"\n🎉 EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"📊 Total invoices processed: {len(results['extracted_data'])}")
            print(f"💾 All data has been saved in multiple formats")
        else:
            print("❌ No data was extracted")
    
    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
        print("💾 Progress has been saved - you can resume later using option 4!")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())