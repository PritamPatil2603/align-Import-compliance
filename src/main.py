import asyncio
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List
import pandas as pd

from config import SystemConfig
from models import ComplianceReport, ESNProcessingResult, ProcessingStatus
from google_services import GoogleServicesManager
from invoice_processor import InvoiceProcessor

class ComplianceSystemOrchestrator:
    """Main orchestrator for the US Import Compliance System - PRODUCTION VERSION"""
    
    def __init__(self, config: SystemConfig):
        self.config = config
        self._setup_logging()
        
        # Initialize components
        self.google_manager = GoogleServicesManager(
            config.GOOGLE_CREDENTIALS_PATH,
            config.GOOGLE_SHEETS_ID
        )
        
        self.invoice_processor = InvoiceProcessor(config)
        self.logger = logging.getLogger(__name__)
        self.logger.info("🚀 Compliance System Orchestrator initialized")
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        Path("logs").mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.config.LOG_LEVEL),
            format=log_format,
            handlers=[
                logging.FileHandler(f"logs/compliance_{datetime.now().strftime('%Y%m%d')}.log"),
                logging.StreamHandler()
            ]
        )
    
    async def process_single_esn(self, esn: str, esn_folder_id: str) -> ESNProcessingResult:
        """Process a single ESN - SIMPLIFIED WORKFLOW"""
        
        start_time = datetime.now()
        self.logger.info(f"🎯 Processing ESN: {esn}")
        
        try:
            # Step 1: Get declared amount from Google Sheets
            declared_amount = self.google_manager.get_esn_declared_amount(esn)
            if declared_amount is None:
                return ESNProcessingResult(
                    esn=esn,
                    status=ProcessingStatus.ERROR,
                    declared_amount=0,
                    calculated_amount=0,
                    difference=0,
                    percentage_difference=0,
                    invoice_count=0,
                    successful_extractions=0,
                    failed_extractions=0,
                    processing_errors=[f"ESN {esn} not found in Google Sheets"]
                )
            
            # Step 2: Get invoice files from Google Drive
            invoice_files = self.google_manager.get_commercial_invoices_files(esn_folder_id)
            if not invoice_files:
                return ESNProcessingResult(
                    esn=esn,
                    status=ProcessingStatus.ERROR,
                    declared_amount=declared_amount,
                    calculated_amount=0,
                    difference=declared_amount,
                    percentage_difference=100,
                    invoice_count=0,
                    successful_extractions=0,
                    failed_extractions=0,
                    processing_errors=[f"No invoice files found for ESN {esn}"]
                )
            
            # Step 3: Download and process invoices
            temp_dir = Path(self.config.TEMP_DIR) / esn
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            downloaded_files = []
            for file_info in invoice_files:
                local_path = temp_dir / file_info['name']
                if self.google_manager.download_file(file_info['id'], str(local_path)):
                    downloaded_files.append(str(local_path))
            
            if not downloaded_files:
                return ESNProcessingResult(
                    esn=esn,
                    status=ProcessingStatus.ERROR,
                    declared_amount=declared_amount,
                    calculated_amount=0,
                    difference=declared_amount,
                    percentage_difference=100,
                    invoice_count=0,
                    successful_extractions=0,
                    failed_extractions=0,
                    processing_errors=[f"Failed to download invoice files for ESN {esn}"]
                )
            
            # Step 4: Extract data from invoices
            extracted_invoices = await self.invoice_processor.process_esn_invoices(esn, downloaded_files)
            
            # Step 5: Calculate results
            calculated_amount = sum(inv.total_usd_amount for inv in extracted_invoices if inv.confidence_level != "ERROR")
            successful_count = len([inv for inv in extracted_invoices if inv.confidence_level != "ERROR"])
            failed_count = len([inv for inv in extracted_invoices if inv.confidence_level == "ERROR"])
            
            # Clean up downloaded files
            for file_path in downloaded_files:
                try:
                    Path(file_path).unlink()
                except:
                    pass
            try:
                temp_dir.rmdir()
            except:
                pass
            
            # Step 6: Determine status
            difference = abs(declared_amount - calculated_amount)
            percentage_diff = (difference / declared_amount * 100) if declared_amount > 0 else 100
            
            if failed_count == len(extracted_invoices):
                status = ProcessingStatus.ERROR
            elif percentage_diff <= self.config.TOLERANCE_PERCENTAGE:
                status = ProcessingStatus.MATCH
            else:
                status = ProcessingStatus.MISMATCH
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Prepare invoice details
            processed_invoices = []
            processing_errors = []
            
            for inv in extracted_invoices:
                processed_invoices.append({
                    "invoice_number": inv.invoice_number,
                    "company_name": inv.company_name,
                    "amount": float(inv.total_usd_amount),
                    "currency": inv.currency,
                    "confidence": inv.confidence_level.value,
                    "notes": inv.extraction_notes
                })
                
                if inv.confidence_level == "ERROR":
                    processing_errors.append(f"Failed to process: {inv.invoice_number}")
            
            result = ESNProcessingResult(
                esn=esn,
                status=status,
                declared_amount=declared_amount,
                calculated_amount=calculated_amount,
                difference=difference,
                percentage_difference=percentage_diff,
                invoice_count=len(extracted_invoices),
                successful_extractions=successful_count,
                failed_extractions=failed_count,
                processed_invoices=processed_invoices,
                processing_errors=processing_errors,
                processing_time_seconds=processing_time
            )
            
            # Log result
            status_icon = "✅" if status == ProcessingStatus.MATCH else "❌" if status == ProcessingStatus.MISMATCH else "⚠️"
            self.logger.info(f"{status_icon} {esn}: {status.value} - Declared: ${declared_amount}, Calculated: ${calculated_amount} ({percentage_diff:.2f}% diff)")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Failed to process ESN {esn}: {e}")
            return ESNProcessingResult(
                esn=esn,
                status=ProcessingStatus.ERROR,
                declared_amount=0,
                calculated_amount=0,
                difference=0,
                percentage_difference=0,
                invoice_count=0,
                successful_extractions=0,
                failed_extractions=0,
                processing_errors=[f"System error: {str(e)}"]
            )
    
    async def process_all_esns(self) -> ComplianceReport:
        """Process all ESNs and generate comprehensive compliance report"""
        
        self.logger.info("🚀 Starting full compliance verification")
        start_time = datetime.now()
        
        # Get all ESN folders
        esn_folders = self.google_manager.get_all_esn_folders()
        
        if not esn_folders:
            self.logger.error("❌ No ESN folders found")
            return self._create_empty_report()
        
        self.logger.info(f"📂 Found {len(esn_folders)} ESN folders")
        
        # Process ESNs with concurrency
        semaphore = asyncio.Semaphore(3)  # Process 3 ESNs at a time
        
        async def process_with_concurrency(esn_info):
            async with semaphore:
                return await self.process_single_esn(esn_info['esn'], esn_info['folder_id'])
        
        # Process all ESNs
        tasks = [process_with_concurrency(esn_info) for esn_info in esn_folders]
        esn_results = await asyncio.gather(*tasks)
        
        # Generate report
        report = self._generate_report(esn_results)
        
        # Save reports
        await self._save_reports(report)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"✅ Completed in {processing_time:.2f} seconds")
        self._log_summary(report)
        
        return report
    
    def _generate_report(self, esn_results: List[ESNProcessingResult]) -> ComplianceReport:
        """Generate compliance report from results"""
        
        total_processed = len(esn_results)
        successful_matches = len([r for r in esn_results if r.status == ProcessingStatus.MATCH])
        discrepancies = len([r for r in esn_results if r.status == ProcessingStatus.MISMATCH])
        errors = len([r for r in esn_results if r.status == ProcessingStatus.ERROR])
        
        total_declared = sum(r.declared_amount for r in esn_results)
        total_calculated = sum(r.calculated_amount for r in esn_results)
        compliance_rate = (successful_matches / total_processed * 100) if total_processed > 0 else 0.0
        
        return ComplianceReport(
            report_id=f"COMPLIANCE_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            total_esns_processed=total_processed,
            successful_matches=successful_matches,
            discrepancies_found=discrepancies,
            processing_errors=errors,
            total_declared_amount=total_declared,
            total_calculated_amount=total_calculated,
            compliance_rate=compliance_rate,
            esn_results=esn_results
        )
    
    async def _save_reports(self, report: ComplianceReport):
        """Save reports in multiple formats"""
        
        output_dir = Path(self.config.OUTPUT_DIR)
        
        # JSON report
        json_file = output_dir / f"{report.report_id}.json"
        with open(json_file, 'w') as f:
            json.dump(report.model_dump(), f, indent=2, default=str)
        
        # Excel report
        excel_file = output_dir / f"{report.report_id}.xlsx"
        self._create_excel_report(report, excel_file)
        
        self.logger.info(f"📊 Reports saved: {json_file}, {excel_file}")
    
    def _create_excel_report(self, report: ComplianceReport, excel_file: Path):
        """Create Excel report"""
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Summary
            summary_df = pd.DataFrame([{
                'Total_ESNs': report.total_esns_processed,
                'Matches': report.successful_matches,
                'Discrepancies': report.discrepancies_found,
                'Errors': report.processing_errors,
                'Compliance_Rate': f"{report.compliance_rate:.2f}%",
                'Total_Declared': f"${report.total_declared_amount:,.2f}",
                'Total_Calculated': f"${report.total_calculated_amount:,.2f}"
            }])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Detailed results
            results_data = []
            for result in report.esn_results:
                results_data.append({
                    'ESN': result.esn,
                    'Status': result.status.value,
                    'Declared_Amount': float(result.declared_amount),
                    'Calculated_Amount': float(result.calculated_amount),
                    'Difference': float(result.difference),
                    'Percentage_Difference': result.percentage_difference,
                    'Invoice_Count': result.invoice_count,
                    'Successful_Extractions': result.successful_extractions
                })
            
            results_df = pd.DataFrame(results_data)
            results_df.to_excel(writer, sheet_name='Results', index=False)
    
    def _create_empty_report(self) -> ComplianceReport:
        """Create empty report"""
        return ComplianceReport(
            report_id=f"EMPTY_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            total_esns_processed=0,
            successful_matches=0,
            discrepancies_found=0,
            processing_errors=0,
            total_declared_amount=0,
            total_calculated_amount=0,
            compliance_rate=0.0,
            esn_results=[]
        )
    
    def _log_summary(self, report: ComplianceReport):
        """Log final summary"""
        self.logger.info("=" * 80)
        self.logger.info("📊 COMPLIANCE VERIFICATION COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"📈 Total ESNs: {report.total_esns_processed}")
        self.logger.info(f"✅ Matches: {report.successful_matches}")
        self.logger.info(f"❌ Discrepancies: {report.discrepancies_found}")
        self.logger.info(f"⚠️  Errors: {report.processing_errors}")
        self.logger.info(f"🎯 Compliance Rate: {report.compliance_rate:.1f}%")
        self.logger.info("=" * 80)

# MAIN EXECUTION
async def main():
    """Main execution function"""
    
    print("🚀 US Import Compliance Verification System")
    print("=" * 60)
    
    try:
        config = SystemConfig()
        if not config.validate():
            return
        
        system = ComplianceSystemOrchestrator(config)
        report = await system.process_all_esns()
        
        print(f"\n🎉 COMPLETED - Compliance Rate: {report.compliance_rate:.1f}%")
        print(f"📁 Reports saved in: {config.OUTPUT_DIR}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n❌ System error: {e}")

if __name__ == "__main__":
    asyncio.run(main())