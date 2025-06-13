# ============================================
# FILE: src/integrated_compliance_system.py
# Complete compliance pipeline combining financial and SKU validation
# ============================================

import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import pandas as pd
import json

from compliance_validator import ComplianceValidator
from sku_validator import SKUValidator
from config import SystemConfig

class IntegratedComplianceSystem:
    """Complete compliance validation system"""
    
    def __init__(self, mongo_uri: str = None, database_name: str = None):
        self.config = SystemConfig()
        
        # Initialize components
        self.compliance_validator = ComplianceValidator()
        
        # Initialize SKU validator if MongoDB credentials provided
        self.sku_validator = None
        if mongo_uri and database_name:
            try:
                self.sku_validator = SKUValidator(mongo_uri, database_name)
                print("✅ SKU validation enabled")
            except Exception as e:
                print(f"⚠️ SKU validation disabled: {e}")
        
        # Output directory
        self.output_dir = Path(self.config.OUTPUT_DIR) / "integrated_compliance"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    async def run_complete_compliance_check(self) -> Dict[str, any]:
        """Run complete compliance pipeline"""
        
        print("🚀 INTEGRATED COMPLIANCE VALIDATION PIPELINE")
        print("=" * 70)
        
        results = {}
        
        try:
            # Phase 1: Financial Compliance Validation
            print("\n💰 PHASE 1: FINANCIAL COMPLIANCE VALIDATION")
            financial_result = await self._run_financial_validation()
            results['financial_compliance'] = financial_result
            
            # Phase 2: SKU Validation (if available)
            if self.sku_validator:
                print("\n🏷️ PHASE 2: SKU COMPLIANCE VALIDATION")
                sku_result = await self._run_sku_validation()
                results['sku_compliance'] = sku_result
            else:
                print("\n⚠️ PHASE 2: SKU VALIDATION SKIPPED (MongoDB not configured)")
            
            # Phase 3: Generate Integrated Reports
            print("\n📊 PHASE 3: INTEGRATED REPORTING")
            integrated_reports = await self._generate_integrated_reports(results)
            results['integrated_reports'] = integrated_reports
            
            # Phase 4: Summary
            print("\n🎯 PHASE 4: EXECUTIVE SUMMARY")
            self._display_executive_summary(results)
            
            return results
            
        except Exception as e:
            print(f"❌ Integrated compliance check failed: {e}")
            raise
    
    async def _run_financial_validation(self):
        """Run financial compliance validation"""
        
        # Find latest CSV
        csv_dir = Path("data/reports/spanish_extractions/csv")
        if not csv_dir.exists():
            raise FileNotFoundError("No extraction CSV files found")
        
        csv_files = list(csv_dir.glob("live_extraction_*.csv"))
        if not csv_files:
            raise FileNotFoundError("No extraction CSV files found")
        
        latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
        print(f"📄 Using CSV: {latest_csv.name}")
        
        # Run financial validation
        report_files = self.compliance_validator.run_full_compliance_validation(str(latest_csv))
        
        return {
            "csv_file": str(latest_csv),
            "report_files": report_files,
            "status": "completed"
        }
    
    async def _run_sku_validation(self):
        """Run SKU compliance validation"""
        
        # Find latest extraction CSV
        csv_dir = Path("data/reports/spanish_extractions/csv")
        latest_csv = max(csv_dir.glob("live_extraction_*.csv"), key=lambda p: p.stat().st_mtime)
        
        # Ask for Excel reference file (optional)
        excel_ref = None
        use_excel = input("Do you have an Excel reference file for SKU validation? (y/n): ").strip().lower()
        
        if use_excel == 'y':
            excel_ref = input("Enter Excel reference file path: ").strip()
            if not Path(excel_ref).exists():
                print("⚠️ Excel file not found, proceeding without reference")
                excel_ref = None
        
        # Run SKU validation
        report_files = self.sku_validator.validate_skus_from_csv(str(latest_csv), excel_ref)
        
        return {
            "csv_file": str(latest_csv),
            "excel_reference": excel_ref,
            "report_files": report_files,
            "status": "completed"
        }
    
    async def _generate_integrated_reports(self, results: Dict):
        """Generate integrated compliance reports"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        integrated_files = {}
        
        try:
            # Master Dashboard
            dashboard_file = self.output_dir / f'master_compliance_dashboard_{timestamp}.xlsx'
            self._create_master_dashboard(results, dashboard_file)
            integrated_files['master_dashboard'] = str(dashboard_file)
            
            # Consolidated Action Items
            action_items_file = self.output_dir / f'consolidated_action_items_{timestamp}.csv'
            self._create_consolidated_action_items(results, action_items_file)
            integrated_files['action_items'] = str(action_items_file)
            
            # Metrics Summary
            metrics_file = self.output_dir / f'compliance_metrics_summary_{timestamp}.json'
            self._create_metrics_summary(results, metrics_file)
            integrated_files['metrics'] = str(metrics_file)
            
            print(f"📊 Generated {len(integrated_files)} integrated reports")
            
            return integrated_files
            
        except Exception as e:
            print(f"⚠️ Error generating integrated reports: {e}")
            return {}
    
    def _create_master_dashboard(self, results: Dict, file_path: Path):
        """Create master compliance dashboard"""
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            
            # Overall Summary
            summary_data = {
                'Report_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Financial_Validation': 'Completed' if 'financial_compliance' in results else 'Not Run',
                'SKU_Validation': 'Completed' if 'sku_compliance' in results else 'Not Run',
                'Total_Phases_Completed': len(results),
                'Overall_Status': 'Complete' if len(results) >= 2 else 'Partial'
            }
            
            # Load financial statistics
            if 'financial_compliance' in results:
                financial_reports = results['financial_compliance']['report_files']
                if 'statistics' in financial_reports:
                    with open(financial_reports['statistics'], 'r') as f:
                        financial_stats = json.load(f)
                    
                    summary_data.update({
                        'Financial_ESNs_Analyzed': financial_stats.get('total_esns_analyzed', 0),
                        'Financial_Compliance_Rate': financial_stats.get('compliance_metrics', {}).get('compliance_rate_percent', 0),
                        'Financial_Total_Difference_USD': financial_stats.get('financial_summary', {}).get('total_difference', 0)
                    })
            
            # Load SKU statistics
            if 'sku_compliance' in results:
                sku_reports = results['sku_compliance']['report_files']
                if 'statistics' in sku_reports:
                    with open(sku_reports['statistics'], 'r') as f:
                        sku_stats = json.load(f)
                    
                    summary_data.update({
                        'SKU_Total_Analyzed': sku_stats.get('total_skus_analyzed', 0),
                        'SKU_Validation_Rate': len([k for k, v in sku_stats.get('validation_summary', {}).items() if k == 'VALIDATED']) / sku_stats.get('total_skus_analyzed', 1) * 100
                    })
            
            summary_df = pd.DataFrame([summary_data])
            summary_df.to_excel(writer, sheet_name='Executive_Summary', index=False)
            
            # Report links
            report_links = []
            
            if 'financial_compliance' in results:
                for report_type, file_path in results['financial_compliance']['report_files'].items():
                    report_links.append({
                        'Category': 'Financial_Compliance',
                        'Report_Type': report_type,
                        'File_Path': file_path
                    })
            
            if 'sku_compliance' in results:
                for report_type, file_path in results['sku_compliance']['report_files'].items():
                    report_links.append({
                        'Category': 'SKU_Compliance',
                        'Report_Type': report_type,
                        'File_Path': file_path
                    })
            
            if report_links:
                links_df = pd.DataFrame(report_links)
                links_df.to_excel(writer, sheet_name='Report_Files', index=False)
    
    def _create_consolidated_action_items(self, results: Dict, file_path: Path):
        """Create consolidated action items"""
        
        all_actions = []
        
        # Financial action items
        if 'financial_compliance' in results:
            financial_reports = results['financial_compliance']['report_files']
            if 'action_items' in financial_reports and Path(financial_reports['action_items']).exists():
                financial_actions = pd.read_csv(financial_reports['action_items'])
                financial_actions['Source'] = 'Financial_Validation'
                all_actions.append(financial_actions)
        
        # TODO: Add SKU action items when available
        
        if all_actions:
            combined_actions = pd.concat(all_actions, ignore_index=True)
            
            # Sort by priority
            priority_order = {'URGENT': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
            combined_actions['Priority_Order'] = combined_actions['Priority'].map(priority_order)
            combined_actions = combined_actions.sort_values('Priority_Order')
            combined_actions = combined_actions.drop('Priority_Order', axis=1)
            
            combined_actions.to_csv(file_path, index=False)
    
    def _create_metrics_summary(self, results: Dict, file_path: Path):
        """Create comprehensive metrics summary"""
        
        metrics = {
            'generation_timestamp': datetime.now().isoformat(),
            'pipeline_summary': {
                'phases_completed': len(results),
                'financial_validation': 'financial_compliance' in results,
                'sku_validation': 'sku_compliance' in results,
                'overall_status': 'complete' if len(results) >= 2 else 'partial'
            }
        }
        
        # Add financial metrics
        if 'financial_compliance' in results:
            financial_reports = results['financial_compliance']['report_files']
            if 'statistics' in financial_reports and Path(financial_reports['statistics']).exists():
                with open(financial_reports['statistics'], 'r') as f:
                    financial_stats = json.load(f)
                metrics['financial_compliance'] = financial_stats
        
        # Add SKU metrics
        if 'sku_compliance' in results:
            sku_reports = results['sku_compliance']['report_files']
            if 'statistics' in sku_reports and Path(sku_reports['statistics']).exists():
                with open(sku_reports['statistics'], 'r') as f:
                    sku_stats = json.load(f)
                metrics['sku_compliance'] = sku_stats
        
        with open(file_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
    
    def _display_executive_summary(self, results: Dict):
        """Display executive summary"""
        
        print("\n🎯 EXECUTIVE COMPLIANCE SUMMARY")
        print("=" * 60)
        
        print(f"📊 Pipeline Status:")
        print(f"   Phases Completed: {len(results)}")
        print(f"   Financial Validation: {'✅' if 'financial_compliance' in results else '❌'}")
        print(f"   SKU Validation: {'✅' if 'sku_compliance' in results else '❌'}")
        print(f"   Integrated Reports: {'✅' if 'integrated_reports' in results else '❌'}")
        
        # Financial summary
        if 'financial_compliance' in results:
            financial_reports = results['financial_compliance']['report_files']
            if 'statistics' in financial_reports:
                with open(financial_reports['statistics'], 'r') as f:
                    financial_stats = json.load(f)
                
                compliance_rate = financial_stats.get('compliance_metrics', {}).get('compliance_rate_percent', 0)
                total_esns = financial_stats.get('total_esns_analyzed', 0)
                
                print(f"\n💰 Financial Compliance:")
                print(f"   ESNs Analyzed: {total_esns}")
                print(f"   Compliance Rate: {compliance_rate:.1f}%")
        
        # SKU summary
        if 'sku_compliance' in results:
            sku_reports = results['sku_compliance']['report_files']
            if 'statistics' in sku_reports:
                with open(sku_reports['statistics'], 'r') as f:
                    sku_stats = json.load(f)
                
                total_skus = sku_stats.get('total_skus_analyzed', 0)
                validated_count = sku_stats.get('validation_summary', {}).get('VALIDATED', 0)
                validation_rate = validated_count / total_skus * 100 if total_skus > 0 else 0
                
                print(f"\n🏷️ SKU Compliance:")
                print(f"   SKUs Analyzed: {total_skus}")
                print(f"   Validation Rate: {validation_rate:.1f}%")
        
        # Next actions
        print(f"\n🚀 Next Actions:")
        if 'integrated_reports' in results:
            integrated_reports = results['integrated_reports']
            print(f"   📊 Review Dashboard: {integrated_reports.get('master_dashboard', 'N/A')}")
            print(f"   📋 Review Action Items: {integrated_reports.get('action_items', 'N/A')}")
        
        print(f"\n📁 All reports saved to: {self.output_dir}")


# Main execution
async def main():
    """Main execution for integrated compliance system"""
    
    print("🚀 INTEGRATED COMPLIANCE VALIDATION SYSTEM")
    print("=" * 60)
    
    try:
        config = SystemConfig()
        
        # Check if MongoDB is configured in .env
        if config.mongodb_configured:
            print(f"✅ MongoDB configured in .env file")
            print(f"📊 Database: {config.MONGODB_DATABASE}")
            print(f"🏢 Organization: {config.MAESA_ORGANIZATION_ID}")
            
            use_config = input("Use MongoDB settings from .env? (y/n): ").strip().lower()
            
            if use_config == 'y':
                system = IntegratedComplianceSystem(config.MONGODB_URI, config.MONGODB_DATABASE)
            else:
                # Manual MongoDB setup
                mongo_uri = input("Enter MongoDB URI: ").strip()
                database_name = input("Enter Database Name: ").strip()
                system = IntegratedComplianceSystem(mongo_uri, database_name)
        else:
            # Ask for MongoDB setup
            print("⚠️ MongoDB not configured in .env file")
            use_sku = input("Enable SKU validation? (y/n): ").strip().lower()
            
            if use_sku == 'y':
                mongo_uri = input("Enter MongoDB URI: ").strip()
                database_name = input("Enter Database Name: ").strip()
                system = IntegratedComplianceSystem(mongo_uri, database_name)
            else:
                system = IntegratedComplianceSystem()
        
        # Run complete validation
        results = await system.run_complete_compliance_check()
        
        print("\n🎉 INTEGRATED COMPLIANCE VALIDATION COMPLETED!")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())