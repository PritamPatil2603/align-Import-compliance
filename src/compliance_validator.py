# ============================================
# FILE: src/compliance_validator.py
# Industry-grade compliance validation and cross-checking system
# ============================================

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
import json

from config import SystemConfig
from google_services import GoogleServicesManager
from export_manager import ExportManager

class ComplianceValidator:
    """Production-grade compliance validation system"""
    
    def __init__(self, config: SystemConfig = None):
        self.config = config or SystemConfig()
        self.output_dir = Path(self.config.OUTPUT_DIR) / "compliance_reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different report types
        for subdir in ['aggregated', 'cross_validation', 'executive_summary', 'detailed_analysis']:
            (self.output_dir / subdir).mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize Google Services for official data
        self.google_manager = GoogleServicesManager(
            self.config.GOOGLE_CREDENTIALS_PATH,
            self.config.GOOGLE_SHEETS_ID
        )
        
        # Compliance thresholds (configurable)
        self.tolerance_percentage = getattr(self.config, 'TOLERANCE_PERCENTAGE', 1.0)
        self.high_risk_threshold = 5.0  # Above 5% = High Risk
        self.medium_risk_threshold = 2.0  # 2-5% = Medium Risk
        
        print(f"🔍 Compliance Validator Initialized")
        print(f"📊 Tolerance: {self.tolerance_percentage}% | Medium Risk: {self.medium_risk_threshold}% | High Risk: {self.high_risk_threshold}%")
    
    def aggregate_extracted_data(self, csv_file_path: str) -> pd.DataFrame:
        """Step 1: Aggregate extracted PDF data by ESN using Total_Amount (avoiding duplicates)"""
        
        print("\n📊 STEP 1: AGGREGATING EXTRACTED PDF DATA")
        print("=" * 60)
        
        try:
            # Load extracted data
            df = pd.read_csv(csv_file_path)
            print(f"✅ Loaded extracted data: {len(df)} records")
            print(f"📋 Columns: {list(df.columns)}")
            
            # Show sample data structure
            print(f"\n📄 Sample data structure:")
            sample_cols = ['ESN', 'PDF_Filename', 'Total_Amount', 'Line_Items_Count']
            available_cols = [col for col in sample_cols if col in df.columns]
            print(df[available_cols].head(3))
            
            # Data quality checks
            total_esns = df['ESN'].nunique()
            total_pdfs = df['PDF_Filename'].nunique()
            total_line_items = len(df)
            
            print(f"\n📊 Data Overview:")
            print(f"   🎯 Unique ESNs: {total_esns}")
            print(f"   📄 Unique PDFs: {total_pdfs}")
            print(f"   📦 Total Line Items: {total_line_items}")
            
            # Clean Total_Amount values
            df['Total_Amount_Clean'] = pd.to_numeric(df['Total_Amount'], errors='coerce')
            
            missing_amounts = df['Total_Amount_Clean'].isna().sum()
            if missing_amounts > 0:
                print(f"⚠️ Warning: {missing_amounts} records with missing Total_Amount")
                # Remove records with missing amounts
                df = df.dropna(subset=['Total_Amount_Clean'])
                print(f"📊 Cleaned data: {len(df)} records remaining")
            
            print(f"\n🔍 CRITICAL FIX: Handling duplicate PDF entries...")
            
            # Show duplicate analysis
            pdf_counts = df.groupby(['ESN', 'PDF_Filename']).size()
            duplicated_pdfs = pdf_counts[pdf_counts > 1]
            
            print(f"📊 PDF Duplication Analysis:")
            print(f"   Total PDF entries: {len(pdf_counts)}")
            print(f"   Unique PDF files: {len(pdf_counts)}")
            print(f"   PDFs with multiple rows: {len(duplicated_pdfs)}")
            
            if len(duplicated_pdfs) > 0:
                print(f"\n🔍 Sample PDFs with multiple line items:")
                for (esn, pdf), count in duplicated_pdfs.head(5).items():
                    pdf_data = df[(df['ESN'] == esn) & (df['PDF_Filename'] == pdf)]
                    total_amount = pdf_data['Total_Amount_Clean'].iloc[0]  # Should be same for all rows
                    line_items = pdf_data['Line_Items_Count'].iloc[0] if 'Line_Items_Count' in df.columns else count
                    print(f"   {esn} | {pdf[:30]:30} | {count} rows | ${total_amount:,.2f} | {line_items} items")
            
            # *** CRITICAL FIX: AGGREGATE BY ESN AND PDF FIRST ***
            print(f"\n🔄 Step 1: Aggregating by ESN + PDF (remove duplicates)...")
            
            # Group by ESN and PDF_Filename, take first Total_Amount (they should all be the same)
            pdf_aggregated = df.groupby(['ESN', 'PDF_Filename']).agg({
                'Total_Amount_Clean': 'first',  # Take first value (all should be same for same PDF)
                'Line_Items_Count': 'first' if 'Line_Items_Count' in df.columns else 'size',
                'Processing_Status': lambda x: 'SUCCESS' if all(x == 'SUCCESS') else 'PARTIAL',
                'Session_ID': 'first'
            }).reset_index()
            
            print(f"✅ Deduplicated PDF data: {len(pdf_aggregated)} unique PDFs")
            
            # Verify deduplication worked
            before_total = df['Total_Amount_Clean'].sum()
            after_total = pdf_aggregated['Total_Amount_Clean'].sum()
            print(f"💰 Value verification:")
            print(f"   Before deduplication: ${before_total:,.2f}")
            print(f"   After deduplication: ${after_total:,.2f}")
            print(f"   Difference: ${before_total - after_total:,.2f} (should be significant if duplicates existed)")
            
            # Step 2: Aggregate by ESN
            print(f"\n🔄 Step 2: Aggregating by ESN...")
            
            esn_aggregated = pdf_aggregated.groupby('ESN').agg({
                'Total_Amount_Clean': 'sum',  # Now sum unique PDF amounts per ESN
                'PDF_Filename': 'count',      # Count unique PDFs
                'Line_Items_Count': 'sum',    # Sum line items across PDFs
                'Processing_Status': lambda x: 'SUCCESS' if all(x == 'SUCCESS') else 'PARTIAL',
                'Session_ID': 'first'
            }).round(2)
            
            esn_aggregated.columns = [
                'PDF_Total_Value', 'PDF_Count', 'Total_Line_Items', 
                'Processing_Status', 'Session_ID'
            ]
            
            esn_aggregated.reset_index(inplace=True)
            
            print(f"✅ Aggregated to {len(esn_aggregated)} ESNs")
            print(f"💰 Total value across all ESNs: ${esn_aggregated['PDF_Total_Value'].sum():,.2f}")
            
            # Show sample aggregated results
            print(f"\n📄 Sample aggregated results:")
            for _, row in esn_aggregated.head(3).iterrows():
                esn = row['ESN']
                total_value = row['PDF_Total_Value']
                pdf_count = row['PDF_Count']
                
                # Show breakdown for this ESN
                esn_pdfs = pdf_aggregated[pdf_aggregated['ESN'] == esn]
                print(f"\n   ESN: {esn}")
                print(f"   Total Value: ${total_value:,.2f}")
                print(f"   PDF Count: {pdf_count}")
                print(f"   PDF Breakdown:")
                for _, pdf_row in esn_pdfs.iterrows():
                    pdf_name = pdf_row['PDF_Filename'][:40]
                    pdf_amount = pdf_row['Total_Amount_Clean']
                    print(f"      {pdf_name:40} | ${pdf_amount:,.2f}")
            
            # Verification: Compare with old method
            print(f"\n🔍 VERIFICATION: Comparing with old Line_Total method...")
            
            if 'Line_Total' in df.columns:
                df['Line_Total_Clean'] = pd.to_numeric(df['Line_Total'], errors='coerce')
                old_method_total = df.groupby('ESN')['Line_Total_Clean'].sum().sum()
                
                print(f"   New method (Total_Amount): ${esn_aggregated['PDF_Total_Value'].sum():,.2f}")
                print(f"   Old method (Line_Total): ${old_method_total:,.2f}")
                print(f"   Difference: ${abs(esn_aggregated['PDF_Total_Value'].sum() - old_method_total):,.2f}")
            
            # Save aggregated data
            aggregated_file = self.output_dir / 'aggregated' / f'esn_aggregated_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            esn_aggregated.to_csv(aggregated_file, index=False)
            print(f"💾 Saved aggregated data: {aggregated_file}")
            
            return esn_aggregated
        
        except Exception as e:
            self.logger.error(f"Error aggregating extracted data: {e}")
            raise
    
    def load_official_data(self) -> pd.DataFrame:
        """Step 2: Load official government/broker data from Google Sheets"""
        
        print("\n📋 STEP 2: LOADING OFFICIAL GOVERNMENT DATA")
        print("=" * 60)
        
        try:
            # Load Google Sheets data
            print("🔄 Connecting to Google Sheets...")
            
            range_name = "Sheet1!A:Z"
            result = self.google_manager.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.google_manager.sheets_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                raise ValueError("No data found in Google Sheets")
            
            # Convert to DataFrame
            df_official = pd.DataFrame(values[1:], columns=values[0])
            df_official.columns = df_official.columns.str.strip()
            
            print(f"✅ Loaded official data: {len(df_official)} records")
            print(f"📋 Available columns: {list(df_official.columns)}")
            
            # Find the correct columns
            esn_column = None
            amount_column = None
            
            for col in df_official.columns:
                col_lower = col.lower().strip()
                
                # ESN Column: Must be "Entry Summary Number" exactly
                if col_lower == 'entry summary number':
                    esn_column = col
                    print(f"✅ Found ESN column: '{col}'")
                
                # Amount Column: Must contain "line tariff goods value amount"
                if 'line' in col_lower and 'tariff' in col_lower and 'goods' in col_lower and 'value' in col_lower and 'amount' in col_lower:
                    amount_column = col
                    print(f"✅ Found Amount column: '{col}'")
            
            if not esn_column or not amount_column:
                # Manual selection fallback (keep existing code)
                # ... existing manual selection code ...
                pass
            
            print(f"✅ Final ESN column: '{esn_column}'")
            print(f"✅ Final Amount column: '{amount_column}'")
            
            # Clean and prepare official data
            df_clean = df_official[[esn_column, amount_column]].copy()
            df_clean.columns = ['ESN', 'Official_Amount']
            
            # Show sample data before cleaning
            print(f"\n📄 Sample official data before cleaning:")
            print(df_clean.head(3))
            
            # Remove empty rows and clean data
            df_clean = df_clean.dropna(subset=['ESN', 'Official_Amount'])
            df_clean = df_clean[df_clean['ESN'].astype(str).str.strip() != '']
            df_clean = df_clean[df_clean['Official_Amount'].astype(str).str.strip() != '']
            
            # Clean ESN format
            df_clean['ESN'] = df_clean['ESN'].astype(str).str.strip()
            
            # Clean amount format (remove $ and commas)
            df_clean['Official_Amount'] = df_clean['Official_Amount'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
            df_clean['Official_Amount'] = pd.to_numeric(df_clean['Official_Amount'], errors='coerce')
            
            # Remove invalid amounts
            df_clean = df_clean.dropna(subset=['Official_Amount'])
            df_clean = df_clean[df_clean['Official_Amount'] > 0]
            
            print(f"\n🔍 CRITICAL FIX: Checking for multiple records per ESN...")
            
            # Check for multiple records per ESN
            esn_counts = df_clean['ESN'].value_counts()
            multiple_records = esn_counts[esn_counts > 1]
            
            print(f"📊 ESN Analysis:")
            print(f"   Total records: {len(df_clean)}")
            print(f"   Unique ESNs: {len(esn_counts)}")
            print(f"   ESNs with multiple records: {len(multiple_records)}")
            
            if len(multiple_records) > 0:
                print(f"\n🔍 Sample ESNs with multiple records:")
                for esn, count in multiple_records.head(5).items():
                    esn_data = df_clean[df_clean['ESN'] == esn]
                    total_amount = esn_data['Official_Amount'].sum()
                    print(f"   {esn}: {count} records, individual amounts: {list(esn_data['Official_Amount'])}, total: ${total_amount:,.2f}")
            
            # *** CRITICAL FIX: AGGREGATE BY ESN ***
            print(f"\n🔄 Aggregating official data by ESN...")
            
            df_aggregated = df_clean.groupby('ESN').agg({
                'Official_Amount': 'sum'  # Sum all amounts for each ESN
            }).reset_index()
            
            print(f"✅ After aggregation:")
            print(f"   Records before: {len(df_clean)}")
            print(f"   Records after: {len(df_aggregated)}")
            print(f"   Total official value: ${df_aggregated['Official_Amount'].sum():,.2f}")
            
            # Show sample aggregated data
            print(f"\n📄 Sample aggregated official data:")
            sample_esns = df_aggregated.head(5)
            for _, row in sample_esns.iterrows():
                esn = row['ESN']
                total_amount = row['Official_Amount']
                
                # Show breakdown for this ESN
                original_records = df_clean[df_clean['ESN'] == esn]
                if len(original_records) > 1:
                    amounts_list = list(original_records['Official_Amount'])
                    print(f"   {esn}: ${total_amount:,.2f} (breakdown: {amounts_list})")
                else:
                    print(f"   {esn}: ${total_amount:,.2f} (single record)")
            
            return df_aggregated
        
        except Exception as e:
            self.logger.error(f"Error loading official data: {e}")
            raise
    
    def cross_validate_data(self, aggregated_data: pd.DataFrame, official_data: pd.DataFrame) -> pd.DataFrame:
        """Step 3: Cross-validate extracted vs official data"""
        
        print("\n🔍 STEP 3: CROSS-VALIDATION ANALYSIS")
        print("=" * 60)
        
        try:
            # Get unique ESNs from both datasets
            extracted_esns = set(aggregated_data['ESN'].str.strip())
            official_esns = set(official_data['ESN'].str.strip())
            
            print(f"📊 Data Comparison:")
            print(f"   📄 Extracted ESNs: {len(extracted_esns)}")
            print(f"   📋 Official ESNs: {len(official_esns)}")
            
            # Find matches and mismatches
            matching_esns = extracted_esns.intersection(official_esns)
            extracted_only = extracted_esns - official_esns
            official_only = official_esns - extracted_esns
            
            print(f"   ✅ Matching ESNs: {len(matching_esns)}")
            print(f"   📄 PDF Only: {len(extracted_only)}")
            print(f"   📋 Official Only: {len(official_only)}")
            
            # Create comprehensive validation dataset
            validation_results = []
            
            # 1. Process matching ESNs (these can be validated)
            print(f"\n🔍 Processing {len(matching_esns)} matching ESNs...")
            
            for esn in matching_esns:
                extracted_row = aggregated_data[aggregated_data['ESN'] == esn].iloc[0]
                official_row = official_data[official_data['ESN'] == esn].iloc[0]
                
                pdf_value = float(extracted_row['PDF_Total_Value'])
                official_value = float(official_row['Official_Amount'])
                
                # Calculate compliance metrics
                difference = abs(pdf_value - official_value)
                percentage_diff = (difference / official_value * 100) if official_value > 0 else 0
                
                # Determine compliance status
                if percentage_diff <= self.tolerance_percentage:
                    compliance_status = "COMPLIANT"
                    risk_level = "LOW"
                elif percentage_diff <= self.medium_risk_threshold:
                    compliance_status = "NON_COMPLIANT"
                    risk_level = "MEDIUM"
                elif percentage_diff <= self.high_risk_threshold:
                    compliance_status = "NON_COMPLIANT"
                    risk_level = "HIGH"
                else:
                    compliance_status = "NON_COMPLIANT"
                    risk_level = "CRITICAL"
                
                validation_results.append({
                    'ESN': esn,
                    'PDF_Total_Value': pdf_value,
                    'Official_Amount': official_value,
                    'Difference_USD': difference,
                    'Percentage_Difference': percentage_diff,
                    'Compliance_Status': compliance_status,
                    'Risk_Level': risk_level,
                    'Data_Status': 'MATCHED',
                    'PDF_Count': extracted_row['PDF_Count'],
                    'Total_Line_Items': extracted_row['Total_Line_Items'],
                    'Processing_Status': extracted_row['Processing_Status'],
                    'Session_ID': extracted_row['Session_ID']
                })
            
            # 2. Process ESNs only in extracted data
            print(f"📄 Processing {len(extracted_only)} PDF-only ESNs...")
            
            for esn in extracted_only:
                extracted_row = aggregated_data[aggregated_data['ESN'] == esn].iloc[0]
                
                validation_results.append({
                    'ESN': esn,
                    'PDF_Total_Value': float(extracted_row['PDF_Total_Value']),
                    'Official_Amount': None,
                    'Difference_USD': None,
                    'Percentage_Difference': None,
                    'Compliance_Status': 'MISSING_OFFICIAL',
                    'Risk_Level': 'UNKNOWN',
                    'Data_Status': 'PDF_ONLY',
                    'PDF_Count': extracted_row['PDF_Count'],
                    'Total_Line_Items': extracted_row['Total_Line_Items'],
                    'Processing_Status': extracted_row['Processing_Status'],
                    'Session_ID': extracted_row['Session_ID']
                })
            
            # 3. Process ESNs only in official data
            print(f"📋 Processing {len(official_only)} official-only ESNs...")
            
            for esn in official_only:
                official_row = official_data[official_data['ESN'] == esn].iloc[0]
                
                validation_results.append({
                    'ESN': esn,
                    'PDF_Total_Value': None,
                    'Official_Amount': float(official_row['Official_Amount']),
                    'Difference_USD': None,
                    'Percentage_Difference': None,
                    'Compliance_Status': 'MISSING_PDF',
                    'Risk_Level': 'UNKNOWN',
                    'Data_Status': 'OFFICIAL_ONLY',
                    'PDF_Count': None,
                    'Total_Line_Items': None,
                    'Processing_Status': 'NOT_PROCESSED',
                    'Session_ID': None
                })
            
            # Convert to DataFrame
            validation_df = pd.DataFrame(validation_results)
            
            # Sort by risk level and percentage difference
            risk_order = {'CRITICAL': 4, 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1, 'UNKNOWN': 0}
            validation_df['Risk_Order'] = validation_df['Risk_Level'].map(risk_order)
            validation_df = validation_df.sort_values(['Risk_Order', 'Percentage_Difference'], ascending=[False, False])
            validation_df = validation_df.drop('Risk_Order', axis=1)
            
            print(f"\n✅ Cross-validation completed!")
            print(f"📊 Validation Summary:")
            print(f"   ✅ Compliant: {len(validation_df[validation_df['Compliance_Status'] == 'COMPLIANT'])}")
            print(f"   ❌ Non-Compliant: {len(validation_df[validation_df['Compliance_Status'] == 'NON_COMPLIANT'])}")
            print(f"   📄 Missing Official: {len(validation_df[validation_df['Compliance_Status'] == 'MISSING_OFFICIAL'])}")
            print(f"   📋 Missing PDF: {len(validation_df[validation_df['Compliance_Status'] == 'MISSING_PDF'])}")
            
            return validation_df
            
        except Exception as e:
            self.logger.error(f"Error in cross-validation: {e}")
            raise
    
    def generate_compliance_reports(self, validation_data: pd.DataFrame) -> Dict[str, str]:
        """Step 4: Generate comprehensive compliance reports"""
        
        print("\n📊 STEP 4: GENERATING COMPLIANCE REPORTS")
        print("=" * 60)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_files = {}
        
        try:
            # 1. Master Compliance Report (CSV)
            master_file = self.output_dir / 'cross_validation' / f'master_compliance_report_{timestamp}.csv'
            validation_data.to_csv(master_file, index=False)
            report_files['master_csv'] = str(master_file)
            print(f"💾 Master compliance report: {master_file}")
            
            # 2. Executive Summary (Excel with multiple sheets)
            executive_file = self.output_dir / 'executive_summary' / f'executive_compliance_summary_{timestamp}.xlsx'
            self._create_executive_excel_report(validation_data, executive_file)
            report_files['executive_excel'] = str(executive_file)
            print(f"💾 Executive summary: {executive_file}")
            
            # 3. Detailed Analysis by Risk Level
            risk_file = self.output_dir / 'detailed_analysis' / f'risk_analysis_{timestamp}.csv'
            self._create_risk_analysis_report(validation_data, risk_file)
            report_files['risk_analysis'] = str(risk_file)
            print(f"💾 Risk analysis: {risk_file}")
            
            # 4. Action Items Report
            action_file = self.output_dir / 'detailed_analysis' / f'action_items_{timestamp}.csv'
            self._create_action_items_report(validation_data, action_file)
            report_files['action_items'] = str(action_file)
            print(f"💾 Action items: {action_file}")
            
            # 5. Statistical Summary (JSON)
            stats_file = self.output_dir / 'executive_summary' / f'compliance_statistics_{timestamp}.json'
            self._create_statistical_summary(validation_data, stats_file)
            report_files['statistics'] = str(stats_file)
            print(f"💾 Statistics: {stats_file}")
            
            return report_files
            
        except Exception as e:
            self.logger.error(f"Error generating reports: {e}")
            raise
    
    def _create_executive_excel_report(self, validation_data: pd.DataFrame, file_path: Path):
        """Create executive Excel report with multiple sheets"""
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            
            # Sheet 1: Executive Dashboard
            dashboard_data = self._calculate_dashboard_metrics(validation_data)
            dashboard_df = pd.DataFrame([dashboard_data])
            dashboard_df.to_excel(writer, sheet_name='Executive_Dashboard', index=False)
            
            # Sheet 2: Compliance Summary
            compliance_summary = validation_data.groupby(['Compliance_Status', 'Risk_Level']).agg({
                'ESN': 'count',
                'PDF_Total_Value': 'sum',
                'Official_Amount': 'sum',
                'Difference_USD': 'sum'
            }).reset_index()
            compliance_summary.to_excel(writer, sheet_name='Compliance_Summary', index=False)
            
            # Sheet 3: High Risk ESNs
            high_risk = validation_data[validation_data['Risk_Level'].isin(['CRITICAL', 'HIGH'])]
            if not high_risk.empty:
                high_risk.to_excel(writer, sheet_name='High_Risk_ESNs', index=False)
            
            # Sheet 4: Missing Data Analysis
            missing_data = validation_data[validation_data['Data_Status'].isin(['PDF_ONLY', 'OFFICIAL_ONLY'])]
            if not missing_data.empty:
                missing_data.to_excel(writer, sheet_name='Missing_Data', index=False)
            
            # Sheet 5: Complete Validation Results
            validation_data.to_excel(writer, sheet_name='Complete_Results', index=False)
    
    def _calculate_dashboard_metrics(self, validation_data: pd.DataFrame) -> Dict:
        """Calculate executive dashboard metrics"""
        
        total_esns = len(validation_data)
        matched_esns = len(validation_data[validation_data['Data_Status'] == 'MATCHED'])
        compliant_esns = len(validation_data[validation_data['Compliance_Status'] == 'COMPLIANT'])
        
        # Financial metrics
        total_pdf_value = validation_data['PDF_Total_Value'].sum()
        total_official_value = validation_data['Official_Amount'].sum()
        total_difference = validation_data['Difference_USD'].sum()
        
        # Risk distribution
        risk_counts = validation_data['Risk_Level'].value_counts().to_dict()
        
        return {
            'Report_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Total_ESNs': total_esns,
            'Matched_ESNs': matched_esns,
            'Match_Rate_Percent': (matched_esns / total_esns * 100) if total_esns > 0 else 0,
            'Compliant_ESNs': compliant_esns,
            'Compliance_Rate_Percent': (compliant_esns / matched_esns * 100) if matched_esns > 0 else 0,
            'Total_PDF_Value_USD': total_pdf_value,
            'Total_Official_Value_USD': total_official_value,
            'Total_Difference_USD': total_difference,
            'Critical_Risk_Count': risk_counts.get('CRITICAL', 0),
            'High_Risk_Count': risk_counts.get('HIGH', 0),
            'Medium_Risk_Count': risk_counts.get('MEDIUM', 0),
            'Low_Risk_Count': risk_counts.get('LOW', 0),
            'Tolerance_Percentage': self.tolerance_percentage
        }
    
    def _create_risk_analysis_report(self, validation_data: pd.DataFrame, file_path: Path):
        """Create detailed risk analysis report"""
        
        risk_analysis = validation_data[validation_data['Data_Status'] == 'MATCHED'].copy()
        
        if not risk_analysis.empty:
            # Add risk categories
            risk_analysis['Financial_Impact'] = risk_analysis['Difference_USD'].apply(
                lambda x: 'HIGH' if x > 10000 else 'MEDIUM' if x > 1000 else 'LOW'
            )
            
            # Sort by risk and financial impact
            risk_analysis = risk_analysis.sort_values(['Risk_Level', 'Difference_USD'], ascending=[False, False])
            
            risk_analysis.to_csv(file_path, index=False)
    
    def _create_action_items_report(self, validation_data: pd.DataFrame, file_path: Path):
        """Create action items report for immediate attention"""
        
        action_items = []
        
        # Critical and High Risk ESNs
        critical_high = validation_data[validation_data['Risk_Level'].isin(['CRITICAL', 'HIGH'])]
        for _, row in critical_high.iterrows():
            action_items.append({
                'Priority': 'URGENT',
                'ESN': row['ESN'],
                'Issue': f"{row['Risk_Level']} Risk - {row['Percentage_Difference']:.2f}% difference",
                'Action_Required': 'Immediate review and investigation',
                'Financial_Impact': f"${row['Difference_USD']:,.2f}",
                'Status': 'PENDING'
            })
        
        # Missing PDF ESNs
        missing_pdf = validation_data[validation_data['Data_Status'] == 'OFFICIAL_ONLY']
        for _, row in missing_pdf.head(10).iterrows():  # Limit to top 10
            action_items.append({
                'Priority': 'HIGH',
                'ESN': row['ESN'],
                'Issue': 'Missing PDF invoice data',
                'Action_Required': 'Locate and process PDF invoices',
                'Financial_Impact': f"${row['Official_Amount']:,.2f}",
                'Status': 'PENDING'
            })
        
        # Missing Official ESNs
        missing_official = validation_data[validation_data['Data_Status'] == 'PDF_ONLY']
        for _, row in missing_official.head(10).iterrows():  # Limit to top 10
            action_items.append({
                'Priority': 'MEDIUM',
                'ESN': row['ESN'],
                'Issue': 'Missing official declaration data',
                'Action_Required': 'Verify ESN in official records',
                'Financial_Impact': f"${row['PDF_Total_Value']:,.2f}",
                'Status': 'PENDING'
            })
        
        if action_items:
            action_df = pd.DataFrame(action_items)
            action_df.to_csv(file_path, index=False)
    
    def _create_statistical_summary(self, validation_data: pd.DataFrame, file_path: Path):
        """Create statistical summary in JSON format"""
        
        stats = {
            'generation_timestamp': datetime.now().isoformat(),
            'total_esns_analyzed': len(validation_data),
            'data_coverage': {
                'matched_esns': len(validation_data[validation_data['Data_Status'] == 'MATCHED']),
                'pdf_only_esns': len(validation_data[validation_data['Data_Status'] == 'PDF_ONLY']),
                'official_only_esns': len(validation_data[validation_data['Data_Status'] == 'OFFICIAL_ONLY'])
            },
            'compliance_metrics': {
                'compliant_count': len(validation_data[validation_data['Compliance_Status'] == 'COMPLIANT']),
                'non_compliant_count': len(validation_data[validation_data['Compliance_Status'] == 'NON_COMPLIANT']),
                'compliance_rate_percent': len(validation_data[validation_data['Compliance_Status'] == 'COMPLIANT']) / len(validation_data[validation_data['Data_Status'] == 'MATCHED']) * 100 if len(validation_data[validation_data['Data_Status'] == 'MATCHED']) > 0 else 0
            },
            'risk_distribution': validation_data['Risk_Level'].value_counts().to_dict(),
            'financial_summary': {
                'total_pdf_value': float(validation_data['PDF_Total_Value'].sum()),
                'total_official_value': float(validation_data['Official_Amount'].sum()),
                'total_difference': float(validation_data['Difference_USD'].sum()),
                'average_difference_percent': float(validation_data[validation_data['Data_Status'] == 'MATCHED']['Percentage_Difference'].mean())
            },
            'processing_summary': {
                'successful_pdfs': len(validation_data[validation_data['Processing_Status'] == 'SUCCESS']),
                'total_pdf_count': int(validation_data['PDF_Count'].sum()),
                'total_line_items': int(validation_data['Total_Line_Items'].sum())
            }
        }
        
        with open(file_path, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
    
    def run_full_compliance_validation(self, extracted_csv_path: str) -> Dict[str, str]:
        """Run complete compliance validation pipeline"""
        
        print("\n🚀 COMPREHENSIVE COMPLIANCE VALIDATION PIPELINE")
        print("=" * 70)
        print(f"📄 Input CSV: {extracted_csv_path}")
        print(f"📊 Tolerance: {self.tolerance_percentage}%")
        print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Aggregate extracted data
            aggregated_data = self.aggregate_extracted_data(extracted_csv_path)
            
            # Step 2: Load official data
            official_data = self.load_official_data()
            
            # Step 3: Cross-validate
            validation_results = self.cross_validate_data(aggregated_data, official_data)
            
            # Step 4: Generate reports
            report_files = self.generate_compliance_reports(validation_results)
            
            # Step 5: Display summary
            self._display_validation_summary(validation_results)
            
            print(f"\n🎉 COMPLIANCE VALIDATION COMPLETED!")
            print(f"📊 Generated {len(report_files)} report files")
            print(f"📁 Reports saved to: {self.output_dir}")
            
            return report_files
            
        except Exception as e:
            self.logger.error(f"Compliance validation failed: {e}")
            raise
    
    def _display_validation_summary(self, validation_data: pd.DataFrame):
        """Display comprehensive validation summary"""
        
        print(f"\n📊 COMPLIANCE VALIDATION SUMMARY")
        print("=" * 60)
        
        # Overall metrics
        total_esns = len(validation_data)
        matched_esns = len(validation_data[validation_data['Data_Status'] == 'MATCHED'])
        compliant_esns = len(validation_data[validation_data['Compliance_Status'] == 'COMPLIANT'])
        
        print(f"🎯 COVERAGE ANALYSIS:")
        print(f"   Total ESNs: {total_esns}")
        print(f"   Matched ESNs: {matched_esns} ({matched_esns/total_esns*100:.1f}%)")
        print(f"   PDF Only: {len(validation_data[validation_data['Data_Status'] == 'PDF_ONLY'])}")
        print(f"   Official Only: {len(validation_data[validation_data['Data_Status'] == 'OFFICIAL_ONLY'])}")
        
        if matched_esns > 0:
            compliance_rate = compliant_esns / matched_esns * 100
            print(f"\n✅ COMPLIANCE ANALYSIS:")
            print(f"   Compliant: {compliant_esns} ({compliance_rate:.1f}%)")
            print(f"   Non-Compliant: {matched_esns - compliant_esns} ({100-compliance_rate:.1f}%)")
            
            # Risk breakdown
            risk_counts = validation_data['Risk_Level'].value_counts()
            print(f"\n⚠️ RISK DISTRIBUTION:")
            for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                count = risk_counts.get(risk_level, 0)
                if count > 0:
                    print(f"   {risk_level}: {count} ESNs")
            
            # Financial impact
            total_difference = validation_data['Difference_USD'].sum()
            avg_difference = validation_data[validation_data['Data_Status'] == 'MATCHED']['Percentage_Difference'].mean()
            
            print(f"\n💰 FINANCIAL IMPACT:")
            print(f"   Total Difference: ${total_difference:,.2f}")
            print(f"   Average Difference: {avg_difference:.2f}%")
            
            # Top risk ESNs
            high_risk = validation_data[
                validation_data['Risk_Level'].isin(['CRITICAL', 'HIGH'])
            ].head(5)
            
            if not high_risk.empty:
                print(f"\n🚨 TOP RISK ESNs:")
                for _, row in high_risk.iterrows():
                    print(f"   {row['ESN']}: {row['Percentage_Difference']:.2f}% (${row['Difference_USD']:,.2f})")


# ============================================
# MAIN EXECUTION FUNCTION
# ============================================

async def main():
    """Main execution for compliance validation"""
    
    print("🔍 COMPLIANCE VALIDATION SYSTEM")
    print("=" * 50)
    
    try:
        # Initialize validator
        validator = ComplianceValidator()
        
        print("Choose validation mode:")
        print("1. 🎯 Validate specific extraction session")
        print("2. 📁 Validate from CSV file path")
        print("3. 🔍 Find and validate latest session")
        
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == "1":
            # Show available sessions
            export_manager = ExportManager(validator.config)
            checkpoints_dir = Path(validator.config.OUTPUT_DIR) / "spanish_extractions" / "checkpoints"
            
            if checkpoints_dir.exists():
                session_files = list(checkpoints_dir.glob("session_*.json"))
                if session_files:
                    print(f"\n📋 Available sessions:")
                    for i, session_file in enumerate(session_files[-10:], 1):  # Show last 10
                        session_id = session_file.stem.replace('session_', '')
                        print(f"   {i}. {session_id}")
                    
                    session_choice = input("\nEnter session number: ").strip()
                    if session_choice.isdigit() and 1 <= int(session_choice) <= len(session_files[-10:]):
                        session_id = session_files[-10:][int(session_choice)-1].stem.replace('session_', '')
                        csv_path = f"data/reports/spanish_extractions/csv/live_extraction_{session_id}.csv"
                        
                        if Path(csv_path).exists():
                            report_files = validator.run_full_compliance_validation(csv_path)
                        else:
                            print(f"❌ CSV file not found: {csv_path}")
                    else:
                        print("❌ Invalid selection")
                else:
                    print("❌ No sessions found")
            else:
                print("❌ No sessions directory found")
        
        elif choice == "2":
            csv_path = input("Enter CSV file path: ").strip()
            if Path(csv_path).exists():
                report_files = validator.run_full_compliance_validation(csv_path)
            else:
                print(f"❌ File not found: {csv_path}")
        
        elif choice == "3":
            # Find latest session automatically
            csv_dir = Path("data/reports/spanish_extractions/csv")
            if csv_dir.exists():
                csv_files = list(csv_dir.glob("live_extraction_*.csv"))
                if csv_files:
                    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
                    print(f"📄 Found latest session: {latest_csv.name}")
                    
                    confirm = input("Validate this session? (y/n): ").strip().lower()
                    if confirm == 'y':
                        report_files = validator.run_full_compliance_validation(str(latest_csv))
                    else:
                        print("❌ Validation cancelled")
                else:
                    print("❌ No extraction sessions found")
            else:
                print("❌ No extractions directory found")
        
        else:
            print("❌ Invalid choice")
    
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())