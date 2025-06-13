# ============================================
# FILE: src/sku_validator.py
# SKU Cross-Verification System for MongoDB validation with Google Sheets
# ============================================

import pandas as pd
import numpy as np
from pymongo import MongoClient
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime
import json
import re
from fuzzywuzzy import fuzz, process
import openpyxl
from openpyxl.styles import PatternFill, Font

from config import SystemConfig
from google_services import GoogleServicesManager

class SKUValidator:
    """SKU cross-verification system for MongoDB vs PDF invoices with Google Sheets integration"""
    
    def __init__(self, mongo_uri: str, database_name: str, config: SystemConfig = None):
        """Initialize SKU validator with MongoDB connection and Google Sheets"""
        
        self.config = config or SystemConfig()
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        
        # Maesa organization ID
        self.organization_id = "dff4dbb5-e2cb-49b3-8ae4-082418ac1db2"
        
        # Setup directories
        self.output_dir = Path(self.config.OUTPUT_DIR) / "sku_validation"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize Google Services for official data
        self.google_manager = GoogleServicesManager(
            self.config.GOOGLE_CREDENTIALS_PATH,
            self.config.GOOGLE_SHEETS_ID
        )
        
        # MongoDB connection
        self.mongo_client = None
        self.database = None
        self._connect_to_mongodb()
        
        # Country code mapping (Google Sheets codes to MongoDB full names)
        self.country_code_mapping = {
            'US': 'United States',
            'MX': 'Mexico', 
            'CA': 'Canada',
            'CN': 'China',
            'KR': 'South Korea',
            'JP': 'Japan',
            'DE': 'Germany',
            'FR': 'France',
            'IT': 'Italy',
            'GB': 'United Kingdom',
            'ES': 'Spain',
            'BR': 'Brazil',
            'IN': 'India',
            'TH': 'Thailand',
            'VN': 'Vietnam',
            'MY': 'Malaysia',
            'SG': 'Singapore',
            'ID': 'Indonesia',
            'PH': 'Philippines',
            'TW': 'Taiwan'
        }
        
        # SPI Code mapping (Google Sheets to FTA names)
        self.spi_code_mapping = {
            'A': 'Generalized System of Preferences (GSP)',
            'AU': 'Australia FTA',
            'BH': 'Bahrain FTA',
            'CA': 'Canada FTA',
            'CL': 'Chile FTA',
            'CO': 'Colombia TPA',
            'D': 'Caribbean Basin Economic Recovery Act (CBERA)',
            'E': 'Caribbean Basin Trade Partnership Act (CBTPA)',
            'IL': 'Israel FTA',
            'JO': 'Jordan FTA',
            'KR': 'Korea FTA',
            'MA': 'Morocco FTA',
            'MX': 'Mexico FTA',
            'OM': 'Oman FTA',
            'PA': 'Panama TPA',
            'PE': 'Peru TPA',
            'P': 'Dominican Republic–Central America FTA (CAFTA-DR)',
            'P+': 'Dominican Republic–Central America FTA (CAFTA-DR)',
            'S': 'United States-Mexico-Canada Agreement',
            'S+': 'United States-Mexico-Canada Agreement',
            'SG': 'Singapore FTA'
        }
        
        print(f"🔍 SKU Validator Initialized")
        print(f"🏢 Organization ID: {self.organization_id}")
    
    def _connect_to_mongodb(self):
        """Establish MongoDB connection"""
        
        try:
            print("🔄 Connecting to MongoDB Atlas...")
            self.mongo_client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=20000,
                maxPoolSize=10
            )
            
            # Test connection
            self.mongo_client.admin.command('ping')
            self.database = self.mongo_client[self.database_name]
            
            print(f"✅ Connected to MongoDB Atlas: {self.database_name}")
            
            # Verify collections with actual names
            collections = self.database.list_collection_names()
            required_collections = ['sku_data', 'sku_duties']
            
            for collection in required_collections:
                if collection in collections:
                    count = self.database[collection].count_documents({})
                    print(f"   📊 {collection}: {count:,} documents")
                else:
                    print(f"   ⚠️ {collection}: Collection not found")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def validate_skus_from_csv(self, csv_file_path: str, use_google_sheets: bool = True) -> Dict[str, str]:
        """Validate SKUs from CSV against MongoDB and Google Sheets"""
        
        print("\n🚀 SKU VALIDATION PIPELINE")
        print("=" * 50)
        
        try:
            # Step 1: Extract SKUs from CSV
            pdf_sku_data = self._extract_skus_from_csv(csv_file_path)
            
            # Step 2: Get MongoDB data (with your actual schema)
            mongodb_data = self._extract_mongodb_sku_data()
            
            # Step 3: Load Google Sheets reference
            google_sheets_data = None
            if use_google_sheets:
                google_sheets_data = self._load_google_sheets_reference()
            
            # Step 4: Cross-validate
            validation_results = self._cross_validate_skus(mongodb_data, pdf_sku_data, google_sheets_data)
            
            # Step 5: Generate reports
            report_files = self._generate_sku_reports(validation_results)
            
            return report_files
            
        except Exception as e:
            self.logger.error(f"SKU validation failed: {e}")
            raise
        
        finally:
            if self.mongo_client:
                self.mongo_client.close()
    
    def _extract_skus_from_csv(self, csv_file_path: str) -> pd.DataFrame:
        """Extract SKUs and related data from PDF extraction CSV"""
        
        print("📄 Extracting SKUs from CSV...")
        
        df = pd.read_csv(csv_file_path)
        print(f"📋 Available columns: {list(df.columns)}")
        
        # Find SKU column (based on your extraction schema)
        sku_columns = ['SKU', 'Client_Reference', 'Reference', 'Product_SKU', 'Item_Code']
        sku_column = None
        
        for col in sku_columns:
            if col in df.columns:
                sku_column = col
                print(f"✅ Found SKU column: '{col}'")
                break
        
        if not sku_column:
            print("Available columns:", list(df.columns))
            # Manual selection
            for i, col in enumerate(df.columns, 1):
                print(f"   {i}. {col}")
            
            choice = input("Enter column number for SKUs: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(df.columns):
                sku_column = df.columns[int(choice) - 1]
                print(f"✅ Selected SKU column: '{sku_column}'")
            else:
                raise ValueError("Invalid column selection")
        
        # Extract SKU data including ESN for cross-referencing
        required_columns = [sku_column, 'ESN']
        available_columns = [col for col in required_columns if col in df.columns]
        
        if 'ESN' not in df.columns:
            print("⚠️ ESN column not found - SKU validation will be limited")
            sku_data = df[[sku_column]].copy()
            sku_data['ESN'] = 'UNKNOWN'
        else:
            sku_data = df[[sku_column, 'ESN']].copy()
        
        sku_data.columns = ['SKU', 'ESN']
        
        # Clean SKU data
        sku_data['SKU'] = sku_data['SKU'].astype(str).str.strip().str.upper()
        sku_data = sku_data[sku_data['SKU'] != 'NAN']
        sku_data = sku_data.dropna(subset=['SKU'])
        
        # Get unique SKUs but keep ESN relationship
        sku_data = sku_data.drop_duplicates()
        
        print(f"✅ Extracted {len(sku_data)} unique SKU-ESN combinations")
        print(f"📄 Sample SKUs: {sku_data['SKU'].head(5).tolist()}")
        
        return sku_data
    
    def _extract_mongodb_sku_data(self) -> pd.DataFrame:
        """Extract SKU data from MongoDB for Maesa organization"""
        
        print("🗄️ Extracting SKU data from MongoDB...")
        
        # Query sku_data collection with your actual field names
        sku_query = {
            "organization_id": self.organization_id,
            "sku_tier": 1  # Only Tier 1 SKUs as per your requirement
        }
        
        print(f"🔍 MongoDB Query: {sku_query}")
        
        sku_cursor = self.database.sku_data.find(sku_query)
        sku_data = list(sku_cursor)
        
        if not sku_data:
            print(f"⚠️ No Tier 1 SKU data found for organization {self.organization_id}")
            # Try alternate query without sku_tier filter
            sku_query_alt = {"organization_id": self.organization_id}
            sku_cursor_alt = self.database.sku_data.find(sku_query_alt)
            sku_data = list(sku_cursor_alt)
            
            if not sku_data:
                raise ValueError(f"No SKU data found for organization {self.organization_id}")
            else:
                print(f"✅ Found {len(sku_data)} SKU records (all tiers)")
        else:
            print(f"✅ Found {len(sku_data)} Tier 1 SKU records")
        
        sku_df = pd.DataFrame(sku_data)
        
        # Show sample structure
        print(f"📋 SKU_Data Fields: {list(sku_df.columns)}")
        sample_cols = ['name', 'country', 'hts_number', '_id']
        available_cols = [col for col in sample_cols if col in sku_df.columns]
        if available_cols:
            print(f"📄 Sample SKU records:")
            print(sku_df[available_cols].head(3))
        
        # Extract SKU IDs for duties lookup
        sku_ids = [str(doc['_id']) for doc in sku_data]
        print(f"🔍 Looking up duties for {len(sku_ids)} SKUs...")
        
        # Query sku_duties collection
        duties_cursor = self.database.sku_duties.find({
            "sku_id": {"$in": sku_ids}
        })
        duties_data = list(duties_cursor)
        
        duties_df = pd.DataFrame(duties_data) if duties_data else pd.DataFrame()
        print(f"✅ Found {len(duties_df)} duty records")
        
        if not duties_df.empty:
            print(f"📋 Sku_Duties Fields: {list(duties_df.columns)}")
        
        # Join SKU data with duties
        sku_df['_id_str'] = sku_df['_id'].astype(str)
        
        if not duties_df.empty:
            combined_df = sku_df.merge(
                duties_df[['sku_id', 'fta']],
                left_on='_id_str',
                right_on='sku_id',
                how='left'
            )
            print(f"✅ Joined data: {len(combined_df)} records")
        else:
            combined_df = sku_df.copy()
            combined_df['fta'] = None
            print(f"⚠️ No duties data found, FTA column will be empty")
        
        # Standardize column names for consistency
        if 'name' in combined_df.columns:
            combined_df['Name'] = combined_df['name']
        if 'country' in combined_df.columns:
            combined_df['Country'] = combined_df['country']
        if 'hts_number' in combined_df.columns:
            combined_df['HTS_Number'] = combined_df['hts_number']
        if 'fta' in combined_df.columns:
            combined_df['FTA'] = combined_df['fta']
        
        print(f"\n✅ MongoDB extraction completed:")
        print(f"   📊 Total SKUs: {len(combined_df)}")
        print(f"   🏷️ SKUs with HTS: {combined_df['HTS_Number'].notna().sum() if 'HTS_Number' in combined_df.columns else 0}")
        print(f"   🌍 SKUs with Country: {combined_df['Country'].notna().sum() if 'Country' in combined_df.columns else 0}")
        print(f"   🤝 SKUs with FTA: {combined_df['FTA'].notna().sum() if 'FTA' in combined_df.columns else 0}")
        
        return combined_df
    
    def _load_google_sheets_reference(self) -> pd.DataFrame:
        """Load official declaration data from Google Sheets"""
        
        print("📊 Loading Google Sheets reference data...")
        
        try:
            # Load Google Sheets data using same method as compliance_validator
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
            
            # Find the required columns for SKU validation
            esn_column = None
            country_column = None
            hts_column = None
            spi_column = None
            
            for col in df_official.columns:
                col_lower = col.lower().strip()
                
                # ESN Column
                if col_lower == 'entry summary number':
                    esn_column = col
                    print(f"✅ Found ESN column: '{col}'")
                
                # Country of Origin Code column
                elif 'country' in col_lower and 'origin' in col_lower and 'code' in col_lower:
                    country_column = col
                    print(f"✅ Found Country column: '{col}'")
                
                # HTS Number column
                elif 'hts' in col_lower and 'number' in col_lower and 'full' in col_lower:
                    hts_column = col
                    print(f"✅ Found HTS column: '{col}'")
                
                # Line SPI Code column
                elif 'line' in col_lower and 'spi' in col_lower and 'code' in col_lower:
                    spi_column = col
                    print(f"✅ Found SPI Code column: '{col}'")
            
            if not esn_column:
                print("⚠️ ESN column not found in Google Sheets")
                return pd.DataFrame()
            
            # Extract relevant columns
            relevant_columns = [esn_column]
            if country_column: relevant_columns.append(country_column)
            if hts_column: relevant_columns.append(hts_column)
            if spi_column: relevant_columns.append(spi_column)
            
            df_clean = df_official[relevant_columns].copy()
            
            # Rename columns for consistency
            column_mapping = {esn_column: 'ESN'}
            if country_column: column_mapping[country_column] = 'Country_Code'
            if hts_column: column_mapping[hts_column] = 'HTS_Number_Official'
            if spi_column: column_mapping[spi_column] = 'SPI_Code'
            
            df_clean = df_clean.rename(columns=column_mapping)
            
            # Clean data
            df_clean = df_clean.dropna(subset=['ESN'])
            df_clean['ESN'] = df_clean['ESN'].astype(str).str.strip()
            df_clean = df_clean[df_clean['ESN'] != '']
            
            # Convert country codes to full names if country column exists
            if 'Country_Code' in df_clean.columns:
                df_clean['Country_Full_Name'] = df_clean['Country_Code'].map(self.country_code_mapping)
                df_clean['Country_Full_Name'] = df_clean['Country_Full_Name'].fillna(df_clean['Country_Code'])
            
            # Convert SPI codes to FTA names if SPI column exists
            if 'SPI_Code' in df_clean.columns:
                df_clean['FTA_Name'] = df_clean['SPI_Code'].map(self.spi_code_mapping)
                df_clean['FTA_Name'] = df_clean['FTA_Name'].fillna(df_clean['SPI_Code'])
            
            print(f"✅ Google Sheets data processed:")
            print(f"   📊 Records: {len(df_clean)}")
            print(f"   📋 Columns: {list(df_clean.columns)}")
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Error loading Google Sheets: {e}")
            print(f"⚠️ Could not load Google Sheets data: {e}")
            return pd.DataFrame()
    
    def _cross_validate_skus(self, mongodb_data: pd.DataFrame, pdf_sku_data: pd.DataFrame, google_sheets_data: pd.DataFrame = None) -> pd.DataFrame:
        """Cross-validate SKUs between sources"""
        
        print("🔍 Cross-validating SKUs...")
        
        validation_results = []
        
        # Create lookup sets from MongoDB
        if 'Name' in mongodb_data.columns:
            mongodb_skus = set(mongodb_data['Name'].astype(str).str.upper().str.strip())
        else:
            print("⚠️ 'Name' column not found in MongoDB data")
            mongodb_skus = set()
        
        print(f"📊 MongoDB SKUs: {len(mongodb_skus)}")
        print(f"📄 PDF SKUs: {len(pdf_sku_data)}")
        print(f"📋 Google Sheets ESNs: {len(google_sheets_data) if google_sheets_data is not None and not google_sheets_data.empty else 0}")
        
        for _, sku_row in pdf_sku_data.iterrows():
            sku = sku_row['SKU']
            esn = sku_row['ESN']
            
            result = {
                'PDF_SKU': sku,
                'ESN': esn,
                'MongoDB_Match': 'NOT_FOUND',
                'MongoDB_SKU_Name': None,
                'MongoDB_Country': None,
                'MongoDB_HTS': None,
                'MongoDB_FTA': None,
                'Official_Country_Code': None,
                'Official_Country_Full': None,
                'Official_HTS': None,
                'Official_SPI_Code': None,
                'Official_FTA_Name': None,
                'Country_Match': None,
                'HTS_Match': None,
                'FTA_Match': None,
                'Overall_Status': 'MISSING_FROM_MONGODB'
            }
            
            # Step 1: Check MongoDB for SKU
            if sku in mongodb_skus:
                mongo_rows = mongodb_data[mongodb_data['Name'].astype(str).str.upper().str.strip() == sku]
                if not mongo_rows.empty:
                    mongo_row = mongo_rows.iloc[0]
                    result.update({
                        'MongoDB_Match': 'EXACT',
                        'MongoDB_SKU_Name': mongo_row.get('Name'),
                        'MongoDB_Country': mongo_row.get('Country'),
                        'MongoDB_HTS': mongo_row.get('HTS_Number'),
                        'MongoDB_FTA': mongo_row.get('FTA'),
                        'Overall_Status': 'FOUND_IN_MONGODB'
                    })
            else:
                # Try fuzzy matching
                if mongodb_skus:
                    fuzzy_matches = process.extract(sku, list(mongodb_skus), scorer=fuzz.ratio, limit=1)
                    if fuzzy_matches and fuzzy_matches[0][1] >= 85:  # 85% similarity threshold
                        best_match = fuzzy_matches[0]
                        mongo_rows = mongodb_data[mongodb_data['Name'].astype(str).str.upper().str.strip() == best_match[0]]
                        if not mongo_rows.empty:
                            mongo_row = mongo_rows.iloc[0]
                            result.update({
                                'MongoDB_Match': f'FUZZY_{best_match[1]}%',
                                'MongoDB_SKU_Name': mongo_row.get('Name'),
                                'MongoDB_Country': mongo_row.get('Country'),
                                'MongoDB_HTS': mongo_row.get('HTS_Number'),
                                'MongoDB_FTA': mongo_row.get('FTA'),
                                'Overall_Status': 'FUZZY_MATCH_MONGODB'
                            })
            
            # Step 2: Check Google Sheets for ESN if available
            if google_sheets_data is not None and not google_sheets_data.empty and esn != 'UNKNOWN':
                official_rows = google_sheets_data[google_sheets_data['ESN'] == esn]
                if not official_rows.empty:
                    official_row = official_rows.iloc[0]
                    result.update({
                        'Official_Country_Code': official_row.get('Country_Code'),
                        'Official_Country_Full': official_row.get('Country_Full_Name'),
                        'Official_HTS': official_row.get('HTS_Number_Official'),
                        'Official_SPI_Code': official_row.get('SPI_Code'),
                        'Official_FTA_Name': official_row.get('FTA_Name')
                    })
                    
                    # Step 3: Cross-validate MongoDB vs Official data
                    if result['MongoDB_Match'] != 'NOT_FOUND':
                        # Check Country match
                        mongo_country = result['MongoDB_Country']
                        official_country = result['Official_Country_Full']
                        
                        if mongo_country and official_country:
                            if mongo_country.upper().strip() == official_country.upper().strip():
                                result['Country_Match'] = 'EXACT'
                            else:
                                result['Country_Match'] = 'MISMATCH'
                        else:
                            result['Country_Match'] = 'MISSING_DATA'
                        
                        # Check HTS match
                        mongo_hts = str(result['MongoDB_HTS']).strip() if result['MongoDB_HTS'] else ''
                        official_hts = str(result['Official_HTS']).strip() if result['Official_HTS'] else ''
                        
                        if mongo_hts and official_hts:
                            if mongo_hts == official_hts:
                                result['HTS_Match'] = 'EXACT'
                            elif mongo_hts[:6] == official_hts[:6]:  # First 6 digits match
                                result['HTS_Match'] = 'PARTIAL'
                            else:
                                result['HTS_Match'] = 'MISMATCH'
                        else:
                            result['HTS_Match'] = 'MISSING_DATA'
                        
                        # Check FTA match
                        mongo_fta = str(result['MongoDB_FTA']).strip() if result['MongoDB_FTA'] else ''
                        official_fta = str(result['Official_FTA_Name']).strip() if result['Official_FTA_Name'] else ''
                        
                        if mongo_fta and official_fta:
                            if mongo_fta.upper() == official_fta.upper():
                                result['FTA_Match'] = 'EXACT'
                            else:
                                result['FTA_Match'] = 'MISMATCH'
                        else:
                            result['FTA_Match'] = 'MISSING_DATA'
                        
                        # Determine overall compliance status
                        if (result['Country_Match'] == 'EXACT' and 
                            result['HTS_Match'] in ['EXACT', 'PARTIAL'] and
                            result['FTA_Match'] in ['EXACT', 'MISSING_DATA']):
                            result['Overall_Status'] = 'VALIDATED'
                        else:
                            result['Overall_Status'] = 'VALIDATION_ISSUES'
            
            validation_results.append(result)
        
        validation_df = pd.DataFrame(validation_results)
        
        # Summary
        status_counts = validation_df['Overall_Status'].value_counts()
        print(f"✅ Validation completed:")
        for status, count in status_counts.items():
            print(f"   {status}: {count}")
        
        return validation_df
    
    def _generate_sku_reports(self, validation_df: pd.DataFrame) -> Dict[str, str]:
        """Generate SKU validation reports"""
        
        print("📊 Generating SKU reports...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_files = {}
        
        # Master CSV report
        csv_file = self.output_dir / f'sku_validation_master_{timestamp}.csv'
        validation_df.to_csv(csv_file, index=False)
        report_files['master_csv'] = str(csv_file)
        
        # Excel report with multiple sheets
        excel_file = self.output_dir / f'sku_validation_report_{timestamp}.xlsx'
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main validation results
            validation_df.to_excel(writer, sheet_name='SKU_Validation', index=False)
            
            # Summary sheet
            summary_data = []
            status_counts = validation_df['Overall_Status'].value_counts()
            total_skus = len(validation_df)
            
            for status, count in status_counts.items():
                percentage = (count / total_skus * 100) if total_skus > 0 else 0
                summary_data.append({
                    'Status': status,
                    'Count': count,
                    'Percentage': f"{percentage:.1f}%"
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Validation Issues
            issues = validation_df[validation_df['Overall_Status'] == 'VALIDATION_ISSUES']
            if not issues.empty:
                issues.to_excel(writer, sheet_name='Validation_Issues', index=False)
            
            # Missing SKUs
            missing = validation_df[validation_df['Overall_Status'] == 'MISSING_FROM_MONGODB']
            if not missing.empty:
                missing.to_excel(writer, sheet_name='Missing_SKUs', index=False)
            
            # Country Mismatches
            country_mismatches = validation_df[validation_df['Country_Match'] == 'MISMATCH']
            if not country_mismatches.empty:
                country_mismatches.to_excel(writer, sheet_name='Country_Mismatches', index=False)
            
            # HTS Mismatches
            hts_mismatches = validation_df[validation_df['HTS_Match'] == 'MISMATCH']
            if not hts_mismatches.empty:
                hts_mismatches.to_excel(writer, sheet_name='HTS_Mismatches', index=False)
        
        report_files['excel_report'] = str(excel_file)
        
        # Statistics JSON
        stats = {
            'generation_timestamp': datetime.now().isoformat(),
            'organization_id': self.organization_id,
            'total_skus_analyzed': len(validation_df),
            'validation_summary': validation_df['Overall_Status'].value_counts().to_dict(),
            'mongodb_match_summary': validation_df['MongoDB_Match'].value_counts().to_dict(),
            'country_match_summary': validation_df['Country_Match'].value_counts().to_dict(),
            'hts_match_summary': validation_df['HTS_Match'].value_counts().to_dict(),
            'fta_match_summary': validation_df['FTA_Match'].value_counts().to_dict(),
            'validation_metrics': {
                'mongodb_found_rate': len(validation_df[validation_df['MongoDB_Match'] != 'NOT_FOUND']) / len(validation_df) * 100,
                'validated_rate': len(validation_df[validation_df['Overall_Status'] == 'VALIDATED']) / len(validation_df) * 100,
                'issues_rate': len(validation_df[validation_df['Overall_Status'] == 'VALIDATION_ISSUES']) / len(validation_df) * 100
            }
        }
        
        stats_file = self.output_dir / f'sku_validation_stats_{timestamp}.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        report_files['statistics'] = str(stats_file)
        
        print(f"✅ Generated {len(report_files)} SKU reports")
        for report_type, file_path in report_files.items():
            print(f"   📄 {report_type}: {file_path}")
        
        return report_files


# Test function
async def main():
    """Test SKU validation with your MongoDB Atlas"""
    
    print("🔍 SKU VALIDATION TEST - MONGODB ATLAS")
    print("=" * 40)
    
    try:
        config = SystemConfig()
        
        # Use config settings from .env
        if config.mongodb_configured:
            print("✅ Using MongoDB settings from .env file")
            print(f"📊 Database: {config.MONGODB_DATABASE}")
            print(f"🏢 Organization: {config.MAESA_ORGANIZATION_ID}")
            
            validator = SKUValidator(config.MONGODB_URI, config.MONGODB_DATABASE)
        else:
            print("❌ MongoDB not configured in .env file")
            print("Please check your MONGODB_URI and MONGODB_DATABASE settings")
            return
        
        # Use specific CSV file as requested
        csv_path = "data/reports/spanish_extractions/csv/live_extraction_20250611_191647.csv"
        
        if Path(csv_path).exists():
            print(f"📄 Using specified CSV: {csv_path}")
            
            # Run validation with Google Sheets integration
            reports = validator.validate_skus_from_csv(csv_path, use_google_sheets=True)
            
            print("\n🎉 SKU Validation Completed!")
            print("Generated reports:")
            for report_type, file_path in reports.items():
                print(f"  📄 {report_type}: {file_path}")
            
            return reports
        else:
            print(f"❌ Specified CSV file not found: {csv_path}")
            print("Available CSV files:")
            csv_dir = Path("data/reports/spanish_extractions/csv")
            if csv_dir.exists():
                csv_files = list(csv_dir.glob("*.csv"))
                for csv_file in csv_files:
                    print(f"  📄 {csv_file.name}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())