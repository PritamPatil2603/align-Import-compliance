# ============================================
# FILE: src/export_manager.py  
# Production-grade export manager with atomic operations and data integrity
# ============================================

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import shutil
import tempfile

class IncrementalExporter:
    """Production-grade incremental exporter with atomic operations"""
    
    def __init__(self, config, session_id: str = None, resume_existing: bool = False):
        self.config = config
        self.session_id = session_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = Path(config.OUTPUT_DIR) / "spanish_extractions"
        self.logger = logging.getLogger(__name__)
        
        # Create directories
        for subdir in ['json', 'csv', 'excel', 'checkpoints', 'temp']:
            (self.output_dir / subdir).mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.checkpoint_file = self.output_dir / 'checkpoints' / f'session_{self.session_id}.json'
        self.live_json_file = self.output_dir / 'json' / f'live_extraction_{self.session_id}.json'
        self.live_csv_file = self.output_dir / 'csv' / f'live_extraction_{self.session_id}.csv'
        self.live_excel_file = self.output_dir / 'excel' / f'live_extraction_{self.session_id}.xlsx'
        
        # Initialize or load session data
        if resume_existing and self.checkpoint_file.exists():
            self._load_existing_session()
        else:
            self._initialize_new_session()
    
    def _initialize_new_session(self):
        """Initialize new session with empty data structure"""
        self.session_data = {
            "session_metadata": {
                "session_id": self.session_id,
                "start_time": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "total_esns_to_process": 0,
                "completed_esns": [],
                "failed_esns": [],
                "current_status": "INITIALIZING",
                "current_esn_in_progress": None
            },
            "extraction_metadata": {
                "extraction_date": datetime.now().isoformat(),
                "total_invoices": 0,
                "total_line_items": 0,
                "successful_extractions": 0,
                "failed_extractions": 0,
                "processing_time_seconds": 0
            },
            "extracted_data": []
        }
        
        self._save_checkpoint()
        self._create_initial_files()
        
        print(f"📥 Initialized new session: {self.session_id}")
        print(f"📁 Progress tracked in: {self.checkpoint_file}")
    
    def _load_existing_session(self):
        """Load existing session data"""
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                self.session_data = json.load(f)
            
            # Clean up any incomplete ESN in progress
            current_esn = self.session_data["session_metadata"].get("current_esn_in_progress")
            if current_esn:
                print(f"🧹 Cleaning up incomplete ESN: {current_esn}")
                self._rollback_incomplete_esn(current_esn)
            
            self.session_data["session_metadata"]["current_esn_in_progress"] = None
            self.session_data["session_metadata"]["current_status"] = "RESUMED"
            self.session_data["session_metadata"]["last_updated"] = datetime.now().isoformat()
            
            self._save_checkpoint()
            
            completed_count = len(self.session_data["session_metadata"]["completed_esns"])
            failed_count = len(self.session_data["session_metadata"]["failed_esns"])
            
            print(f"📥 Loaded existing session: {self.session_id}")
            print(f"📥 Loaded existing progress: {completed_count} completed, {failed_count} failed")
            
        except Exception as e:
            self.logger.error(f"Error loading session {self.session_id}: {e}")
            print(f"❌ Failed to load session. Starting new session.")
            self._initialize_new_session()
    
    def _rollback_incomplete_esn(self, esn: str):
        """Remove data from incomplete ESN to maintain consistency"""
        original_count = len(self.session_data["extracted_data"])
        
        # Remove all data for the incomplete ESN
        self.session_data["extracted_data"] = [
            invoice for invoice in self.session_data["extracted_data"] 
            if invoice.get('esn') != esn
        ]
        
        removed_count = original_count - len(self.session_data["extracted_data"])
        
        # Update metadata
        self.session_data["extraction_metadata"]["total_invoices"] -= removed_count
        
        # Recalculate line items
        total_line_items = sum(
            len(invoice.get('line_items', [])) 
            for invoice in self.session_data["extracted_data"]
        )
        self.session_data["extraction_metadata"]["total_line_items"] = total_line_items
        
        if removed_count > 0:
            print(f"🧹 Removed {removed_count} incomplete invoices from ESN {esn}")
    
    def _create_initial_files(self):
        """Create initial empty export files"""
        # JSON
        with open(self.live_json_file, 'w', encoding='utf-8') as f:
            json.dump(self.session_data, f, indent=2, ensure_ascii=False, default=str)
        
        # CSV with headers
        headers = [
            'ESN', 'PDF_Filename', 'Invoice_Date', 'Supplier', 'Total_Amount', 
            'Currency', 'Line_Items_Count', 'Processing_Status', 'Session_ID', 
            'Processing_Time', 'SKU', 'Description', 'Quantity', 'Unit_Price', 'Line_Total'
        ]
        pd.DataFrame(columns=headers).to_csv(self.live_csv_file, index=False)
        
        # Excel with initial structure
        self._update_live_excel()
    
    def initialize_processing(self, total_esns: int, esn_list: List[str]):
        """Set total ESNs for progress tracking"""
        self.session_data["session_metadata"]["total_esns_to_process"] = total_esns
        self.session_data["session_metadata"]["esn_list"] = esn_list
        self.session_data["session_metadata"]["current_status"] = "PROCESSING"
        self._save_checkpoint()
        
        print(f"📊 Session {self.session_id}: Processing {total_esns} ESN folders")
        print(f"📁 Progress file: {self.checkpoint_file}")
    
    def start_esn_processing(self, esn: str):
        """Mark ESN as currently being processed"""
        self.session_data["session_metadata"]["current_esn_in_progress"] = esn
        self.session_data["session_metadata"]["last_updated"] = datetime.now().isoformat()
        self._save_checkpoint()
        
        print(f"🔄 Started processing ESN: {esn}")
    
    def add_esn_data(self, esn: str, invoices: List[Dict], processing_time: float):
        """Add complete ESN data atomically"""
        try:
            print(f"💾 Saving data for ESN: {esn} ({len(invoices)} invoices)")
            
            # Mark ESN processing start
            self.start_esn_processing(esn)
            
            # Add invoices to main data
            for invoice in invoices:
                invoice['session_id'] = self.session_id
                invoice['esn_processing_time'] = processing_time
                self.session_data['extracted_data'].append(invoice)
            
            # Update metadata
            self.session_data["session_metadata"]["completed_esns"].append({
                "esn": esn,
                "completion_time": datetime.now().isoformat(),
                "invoices_count": len(invoices),
                "processing_time": processing_time
            })
            
            self.session_data["extraction_metadata"]["total_invoices"] += len(invoices)
            total_line_items = sum(len(inv.get('line_items', [])) for inv in invoices)
            self.session_data["extraction_metadata"]["total_line_items"] += total_line_items
            self.session_data["extraction_metadata"]["successful_extractions"] += len(invoices)
            
            # Mark ESN as completed (no longer in progress)
            self.session_data["session_metadata"]["current_esn_in_progress"] = None
            self.session_data["session_metadata"]["last_updated"] = datetime.now().isoformat()
            
            # Calculate progress
            completed = len(self.session_data["session_metadata"]["completed_esns"])
            total = self.session_data["session_metadata"]["total_esns_to_process"]
            progress_pct = (completed / total * 100) if total > 0 else 0
            
            # 🚀 ATOMIC SAVE: Save to all formats atomically
            self._atomic_save_all_formats()
            
            print(f"✅ ESN {esn} completed atomically")
            print(f"📊 Progress: {completed}/{total} ESNs ({progress_pct:.1f}%)")
            print(f"📄 Total invoices: {self.session_data['extraction_metadata']['total_invoices']}")
            
        except Exception as e:
            self.logger.error(f"Error saving ESN {esn}: {e}")
            # Rollback the failed ESN
            self._rollback_incomplete_esn(esn)
            self.add_failed_esn(esn, str(e))
    
    def add_failed_esn(self, esn: str, error: str):
        """Record a failed ESN"""
        self.session_data["session_metadata"]["failed_esns"].append({
            "esn": esn,
            "error": error,
            "failure_time": datetime.now().isoformat()
        })
        self.session_data["extraction_metadata"]["failed_extractions"] += 1
        
        # Clear in-progress marker
        self.session_data["session_metadata"]["current_esn_in_progress"] = None
        self.session_data["session_metadata"]["last_updated"] = datetime.now().isoformat()
        
        self._save_checkpoint()
        print(f"❌ Failed ESN recorded: {esn} - {error}")
    
    def _atomic_save_all_formats(self):
        """Save all formats atomically using temporary files"""
        try:
            temp_dir = self.output_dir / 'temp'
            
            # Create temporary files
            temp_checkpoint = temp_dir / f'checkpoint_{self.session_id}.tmp'
            temp_json = temp_dir / f'json_{self.session_id}.tmp'
            temp_csv = temp_dir / f'csv_{self.session_id}.tmp'
            temp_excel = temp_dir / f'excel_{self.session_id}.tmp'
            
            # Save to temporary files first
            with open(temp_checkpoint, 'w', encoding='utf-8') as f:
                json.dump(self.session_data, f, indent=2, ensure_ascii=False, default=str)
            
            with open(temp_json, 'w', encoding='utf-8') as f:
                json.dump(self.session_data, f, indent=2, ensure_ascii=False, default=str)
            
            self._save_csv_to_file(temp_csv)
            self._save_excel_to_file(temp_excel)
            
            # Atomic move: replace original files
            shutil.move(str(temp_checkpoint), str(self.checkpoint_file))
            shutil.move(str(temp_json), str(self.live_json_file))
            shutil.move(str(temp_csv), str(self.live_csv_file))
            shutil.move(str(temp_excel), str(self.live_excel_file))
            
        except Exception as e:
            self.logger.error(f"Error in atomic save: {e}")
            raise
    
    def _save_checkpoint(self):
        """Save checkpoint file"""
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.session_data, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    def _save_csv_to_file(self, file_path: Path):
        """Save CSV with proper format"""
        flattened_data = []
        
        for invoice in self.session_data['extracted_data']:
            base_row = {
                'ESN': invoice.get('esn'),
                'PDF_Filename': invoice.get('pdf_filename'),
                'Invoice_Date': invoice.get('fecha_hora'),
                'Supplier': invoice.get('supplier'),
                'Total_Amount': invoice.get('total_usd_amount'),
                'Currency': invoice.get('currency', 'USD'),
                'Line_Items_Count': len(invoice.get('line_items', [])),
                'Processing_Status': 'SUCCESS',
                'Session_ID': self.session_id,
                'Processing_Time': invoice.get('esn_processing_time', 0)
            }
            
            line_items = invoice.get('line_items', [])
            if line_items:
                for item in line_items:
                    row = base_row.copy()
                    row.update({
                        'SKU': item.get('sku'),
                        'Description': item.get('description'),
                        'Quantity': item.get('quantity'),
                        'Unit_Price': item.get('unit_price'),
                        'Line_Total': item.get('line_total')
                    })
                    flattened_data.append(row)
            else:
                row = base_row.copy()
                row.update({
                    'SKU': None,
                    'Description': None,
                    'Quantity': None,
                    'Unit_Price': None,
                    'Line_Total': None
                })
                flattened_data.append(row)
        
        df = pd.DataFrame(flattened_data)
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    def _save_excel_to_file(self, file_path: Path):
        """Save Excel with multiple sheets"""
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Sheet 1: Session Progress
                progress_data = [{
                    'Session_ID': self.session_id,
                    'Start_Time': self.session_data["session_metadata"]["start_time"],
                    'Last_Updated': self.session_data["session_metadata"]["last_updated"],
                    'ESNs_Completed': len(self.session_data["session_metadata"]["completed_esns"]),
                    'ESNs_Failed': len(self.session_data["session_metadata"]["failed_esns"]),
                    'Total_ESNs': self.session_data["session_metadata"]["total_esns_to_process"],
                    'Total_Invoices': self.session_data["extraction_metadata"]["total_invoices"],
                    'Total_Line_Items': self.session_data["extraction_metadata"]["total_line_items"],
                    'Status': self.session_data["session_metadata"]["current_status"]
                }]
                pd.DataFrame(progress_data).to_excel(writer, sheet_name='Session_Progress', index=False)
                
                # Sheet 2: Invoice Summary
                invoice_summary = []
                for invoice in self.session_data['extracted_data']:
                    invoice_summary.append({
                        'ESN': invoice.get('esn'),
                        'PDF_Filename': invoice.get('pdf_filename'),
                        'Supplier': invoice.get('supplier'),
                        'Invoice_Date': invoice.get('fecha_hora'),
                        'Total_Amount': invoice.get('total_usd_amount'),
                        'Line_Items_Count': len(invoice.get('line_items', []))
                    })
                
                if invoice_summary:
                    pd.DataFrame(invoice_summary).to_excel(writer, sheet_name='Invoice_Summary', index=False)
                
        except Exception as e:
            self.logger.error(f"Error saving Excel: {e}")
    
    def _update_live_excel(self):
        """Update live Excel file"""
        self._save_excel_to_file(self.live_excel_file)
    
    def get_completed_esns(self) -> List[str]:
        """Get list of completed ESN IDs"""
        return [item['esn'] for item in self.session_data["session_metadata"]["completed_esns"]]
    
    def get_failed_esns(self) -> List[str]:
        """Get list of failed ESN IDs"""
        return [item['esn'] for item in self.session_data["session_metadata"]["failed_esns"]]
    
    def get_total_invoices(self) -> int:
        """Get total number of invoices processed"""
        return self.session_data["extraction_metadata"]["total_invoices"]
    
    def get_total_line_items(self) -> int:
        """Get total number of line items processed"""
        return self.session_data["extraction_metadata"]["total_line_items"]
    
    def get_average_processing_time(self) -> float:
        """Calculate average processing time per ESN"""
        completed = self.session_data["session_metadata"]["completed_esns"]
        if not completed:
            return 0
        
        total_time = sum(item['processing_time'] for item in completed)
        return total_time / len(completed)
    
    def finalize_session(self):
        """Mark session as completed"""
        self.session_data["session_metadata"]["current_status"] = "COMPLETED"
        self.session_data["session_metadata"]["end_time"] = datetime.now().isoformat()
        self.session_data["session_metadata"]["current_esn_in_progress"] = None
        
        # Calculate final statistics
        start_time = datetime.fromisoformat(self.session_data["session_metadata"]["start_time"])
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        self.session_data["extraction_metadata"]["processing_time_seconds"] = total_time
        
        self._atomic_save_all_formats()
        
        print(f"🎉 Session {self.session_id} finalized!")
        print(f"⏱️ Total time: {total_time/60:.1f} minutes")
    
    def get_final_results(self) -> Dict[str, Any]:
        """Get final results dictionary"""
        return {
            'session_metadata': self.session_data["session_metadata"],
            'extraction_metadata': self.session_data["extraction_metadata"],
            'extracted_data': self.session_data["extracted_data"]
        }


class ExportManager:
    """Main export manager that creates and manages incremental exporters"""
    
    def __init__(self, config):
        self.config = config
        self.output_dir = Path(config.OUTPUT_DIR)
        self.logger = logging.getLogger(__name__)
        
        # Create output directories
        self.exports_dir = self.output_dir / "spanish_extractions"
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        
        for subdir in ['csv', 'excel', 'json', 'checkpoints', 'temp']:
            (self.exports_dir / subdir).mkdir(exist_ok=True)
    
    def create_incremental_exporter(self, session_id: str = None) -> IncrementalExporter:
        """Create a new incremental exporter"""
        resume_existing = session_id is not None
        return IncrementalExporter(self.config, session_id, resume_existing)
    
    def find_resumable_sessions(self) -> List[Dict]:
        """Find sessions that can be resumed"""
        checkpoints_dir = self.exports_dir / "checkpoints"
        if not checkpoints_dir.exists():
            return []
        
        resumable = []
        for progress_file in checkpoints_dir.glob("session_*.json"):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                session_metadata = data.get("session_metadata", {})
                if session_metadata.get("current_status") in ["PROCESSING", "INITIALIZING", "RESUMED"]:
                    completed = len(session_metadata.get("completed_esns", []))
                    total = session_metadata.get("total_esns_to_process", 0)
                    
                    resumable.append({
                        "session_id": session_metadata.get("session_id"),
                        "progress_file": str(progress_file),
                        "completed_esns": completed,
                        "total_esns": total,
                        "last_updated": session_metadata.get("last_updated"),
                        "data": data
                    })
            except Exception:
                continue
        
        return resumable
    
    # Keep existing methods for backward compatibility
    async def export_all_formats(self, results: Dict[str, Any], filename_prefix: str = "all_esn") -> Dict[str, str]:
        """Legacy export method for backward compatibility"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        exported_files = {}
        
        try:
            extracted_data = results['extracted_data']
            
            if not extracted_data:
                self.logger.warning("No data to export")
                return {}
            
            # Export to all formats
            json_path = await self._export_to_json(results, filename_prefix, timestamp)
            exported_files['json'] = json_path
            
            csv_path = await self._export_to_csv(extracted_data, filename_prefix, timestamp)
            exported_files['csv'] = csv_path
            
            excel_path = await self._export_to_excel(results, filename_prefix, timestamp)
            exported_files['excel'] = excel_path
            
            return exported_files
            
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            return {}
    
    async def _export_to_json(self, results: Dict[str, Any], prefix: str, timestamp: str) -> str:
        """Export complete results to JSON"""
        json_file = self.exports_dir / "json" / f"{prefix}_extraction_{timestamp}.json"
        
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"JSON exported: {json_file}")
            return str(json_file)
            
        except Exception as e:
            self.logger.error(f"Error exporting JSON: {e}")
            raise
    
    async def _export_to_csv(self, extracted_data: List[Dict], prefix: str, timestamp: str) -> str:
        """Export flattened data to CSV"""
        csv_file = self.exports_dir / "csv" / f"{prefix}_extraction_{timestamp}.csv"
        
        try:
            # Use same logic as incremental exporter
            exporter = IncrementalExporter(self.config)
            exporter.session_data['extracted_data'] = extracted_data
            exporter._save_csv_to_file(csv_file)
            
            self.logger.info(f"CSV exported: {csv_file}")
            return str(csv_file)
            
        except Exception as e:
            self.logger.error(f"Error exporting CSV: {e}")
            raise
    
    async def _export_to_excel(self, results: Dict[str, Any], prefix: str, timestamp: str) -> str:
        """Export to Excel with multiple sheets"""
        excel_file = self.exports_dir / "excel" / f"{prefix}_extraction_{timestamp}.xlsx"
        
        try:
            # Use same logic as incremental exporter
            exporter = IncrementalExporter(self.config)
            exporter.session_data = results
            exporter._save_excel_to_file(excel_file)
            
            self.logger.info(f"Excel exported: {excel_file}")
            return str(excel_file)
            
        except Exception as e:
            self.logger.error(f"Error exporting Excel: {e}")
            raise