# ============================================
# FILE: src/invoice_processor.py
# PRODUCTION-READY optimized invoice processor with enhanced line item support
# ============================================

import asyncio
import logging
import time
import hashlib
import json
import os
from typing import List, Optional, Dict, Any
from decimal import Decimal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import gc

from llama_parse import LlamaParse
from llama_index.llms.openai import OpenAI
from llama_index.core.prompts import ChatPromptTemplate

from models import CommercialInvoiceData, ConfidenceLevel, EnhancedInvoiceData, LineItem, InvoiceExtractionResult
from config import SystemConfig

logger = logging.getLogger(__name__)

class InvoiceCache:
    """Advanced caching system for invoice processing results"""
    
    def __init__(self, cache_dir: str = "data/cache", max_cache_size: int = 1000):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_cache_size = max_cache_size
        self.cache_index_file = self.cache_dir / "cache_index.json"
        self._load_cache_index()
    
    def _load_cache_index(self):
        """Load cache index for efficient cache management"""
        try:
            if self.cache_index_file.exists():
                with open(self.cache_index_file, 'r') as f:
                    self.cache_index = json.load(f)
            else:
                self.cache_index = {}
        except Exception:
            self.cache_index = {}
    
    def _save_cache_index(self):
        """Save cache index"""
        try:
            with open(self.cache_index_file, 'w') as f:
                json.dump(self.cache_index, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save cache index: {e}")
    
    def _get_file_hash(self, file_path: str) -> str:
        """Generate hash for file caching with metadata"""
        try:
            # Include file size and modification time in hash for better cache invalidation
            stat = os.stat(file_path)
            content_hash = hashlib.md5()
            
            # Read file in chunks to handle large files efficiently
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    content_hash.update(chunk)
            
            # Combine content hash with metadata
            meta_string = f"{stat.st_size}_{stat.st_mtime}_{content_hash.hexdigest()}"
            return hashlib.md5(meta_string.encode()).hexdigest()
        
        except Exception as e:
            logger.warning(f"Error generating file hash: {e}")
            # Fallback to simple content hash
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
    
    def get_cache_path(self, file_hash: str) -> Path:
        """Get cache file path"""
        return self.cache_dir / f"{file_hash}.json"
    
    def load_from_cache(self, file_path: str) -> Optional[CommercialInvoiceData]:
        """Load extraction result from cache"""
        try:
            file_hash = self._get_file_hash(file_path)
            cache_path = self.get_cache_path(file_hash)
            
            if not cache_path.exists():
                return None
            
            # Check cache age (30 days max)
            cache_age = time.time() - cache_path.stat().st_mtime
            if cache_age > 30 * 24 * 3600:  # 30 days
                cache_path.unlink(missing_ok=True)
                return None
            
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
            
            # Update cache index
            self.cache_index[file_hash] = {
                'file_path': str(file_path),
                'cached_at': cache_data.get('cached_at', time.time()),
                'last_accessed': time.time()
            }
            
            return CommercialInvoiceData(
                invoice_number=cache_data['invoice_number'],
                company_name=cache_data['company_name'],
                total_usd_amount=Decimal(str(cache_data['total_usd_amount'])),
                currency=cache_data['currency'],
                confidence_level=ConfidenceLevel(cache_data['confidence_level']),
                extraction_notes=cache_data.get('extraction_notes'),
                amount_source_text=cache_data.get('amount_source_text')
            )
            
        except Exception as e:
            logger.warning(f"Failed to load from cache: {e}")
            return None
    
    def save_to_cache(self, file_path: str, result: CommercialInvoiceData):
        """Save extraction result to cache with size management"""
        try:
            file_hash = self._get_file_hash(file_path)
            cache_path = self.get_cache_path(file_hash)
            
            cache_data = {
                'invoice_number': result.invoice_number,
                'company_name': result.company_name,
                'total_usd_amount': str(result.total_usd_amount),
                'currency': result.currency,
                'confidence_level': result.confidence_level.value,
                'extraction_notes': result.extraction_notes,
                'amount_source_text': result.amount_source_text,
                'cached_at': time.time(),
                'file_path': str(file_path)
            }
            
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Update cache index
            self.cache_index[file_hash] = {
                'file_path': str(file_path),
                'cached_at': time.time(),
                'last_accessed': time.time()
            }
            
            # Manage cache size
            self._manage_cache_size()
            
        except Exception as e:
            logger.warning(f"Failed to save to cache: {e}")
    
    def _manage_cache_size(self):
        """Manage cache size by removing oldest entries"""
        try:
            if len(self.cache_index) > self.max_cache_size:
                # Sort by last accessed time and remove oldest
                sorted_entries = sorted(
                    self.cache_index.items(),
                    key=lambda x: x[1].get('last_accessed', 0)
                )
                
                # Remove oldest 10% of entries
                remove_count = max(1, len(self.cache_index) // 10)
                
                for file_hash, _ in sorted_entries[:remove_count]:
                    cache_path = self.get_cache_path(file_hash)
                    cache_path.unlink(missing_ok=True)
                    del self.cache_index[file_hash]
            
            self._save_cache_index()
            
        except Exception as e:
            logger.warning(f"Error managing cache size: {e}")

class RetryManager:
    """Intelligent retry logic for API calls"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
    
    async def retry_with_backoff(self, func, *args, **kwargs):
        """Retry function with exponential backoff"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            
            except Exception as e:
                last_exception = e
                
                if attempt == self.max_retries:
                    logger.error(f"Max retries exceeded: {e}")
                    raise e
                
                # Exponential backoff with jitter
                delay = self.base_delay * (2 ** attempt) * (0.5 + 0.5 * (time.time() % 1))
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s")
                await asyncio.sleep(delay)
        
        raise last_exception

class OptimizedInvoiceProcessor:
    """Production-ready optimized invoice processor with enhanced line item support"""
    
    def __init__(self, config: SystemConfig):
        self.config = config
        self.cache = InvoiceCache()
        self.retry_manager = RetryManager()
        
        # Performance tracking
        self.processing_stats = {
            'total_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_processing_time': 0
        }
        
        # OPTIMIZED LlamaParse settings for speed
        self.parser = LlamaParse(
            api_key=config.LLAMA_CLOUD_API_KEY,
            result_type="markdown",
            premium_mode=True,           
            language="es",                
            check_interval=2,
            num_workers=6,
            parsing_instruction="""Extract invoice data from this commercial invoice.""",
            verbose=False
        )
        
        # OPTIMIZED OpenAI settings for speed
        self.llm = OpenAI(
            api_key=config.OPENAI_API_KEY,
            model="gpt-4o",
            temperature=0.1,
            max_tokens=900,               
        )
        
        # Optimized extraction prompt
        self.extraction_prompt = ChatPromptTemplate.from_messages([
            ("system", """Extract invoice data from this Spanish commercial invoice (FACTURA COMERCIAL).

Look for these specific fields:
1. Supplier/Company name (Emisor) - The company issuing the invoice
2. REF CLIENTE - Client reference number or SKU
3. DESCRIPCIÓN DEL MATERIAL - Material/product description
4. Fecha y hora de emisión - Invoice date and time
5. CANTIDAD TOTAL - Total quantity/units
6. VALOR UNITARIO - Unit price/value
7. Total USD - Total amount in USD dollars

Pay special attention to tables containing line items.
Convert any monetary amounts to numbers without currency symbols.

CONFIDENCE:
- HIGH: Clear USD total, unambiguous
- MEDIUM: Requires calculation or conversion
- LOW: Estimated or unclear
- ERROR: Cannot determine amount"""),
            
            ("user", """Extract these fields from this commercial invoice:

1. FINAL TOTAL USD AMOUNT (primary target - most important)
2. COMPANY/SUPPLIER NAME (Emisor)
3. CLIENT REFERENCE (REF CLIENTE - SKU, product ID, item number)
4. MATERIAL DESCRIPTION (DESCRIPCIÓN DEL MATERIAL - product description)
5. DATE TIME (Fecha y hora de emisión - invoice date and time)
6. TOTAL QUANTITY (CANTIDAD TOTAL - total units/quantity)
7. UNIT VALUE (VALOR UNITARIO - unit price/value)

Focus primarily on the TOTAL USD AMOUNT. Other fields are optional but helpful for audit purposes.

Invoice content: {invoice_content}""")
        ])
    
    async def process_single_invoice(self, pdf_path: str, esn: str) -> CommercialInvoiceData:
        """Optimized single invoice processing with comprehensive error handling"""
        
        invoice_filename = Path(pdf_path).name
        start_time = time.time()
        
        try:
            # Update stats
            self.processing_stats['total_processed'] += 1
            
            # Step 1: Check cache first
            cached_result = self.cache.load_from_cache(pdf_path)
            if cached_result:
                self.processing_stats['cache_hits'] += 1
                cache_time = time.time() - start_time
                logger.info(f"🟡 CACHED: {invoice_filename} = ${cached_result.total_usd_amount} ({cache_time:.1f}s)")
                return cached_result
            
            self.processing_stats['cache_misses'] += 1
            logger.info(f"📄 Processing: {invoice_filename}")
            
            # Step 2: Parse PDF with retry logic
            docs = await self.retry_manager.retry_with_backoff(
                self._parse_pdf_with_timeout, pdf_path
            )
            
            if not docs:
                raise ValueError("No content extracted from PDF")
            
            # Step 3: Prepare content efficiently
            invoice_content = self._prepare_invoice_content(docs)
            
            # Step 4: Extract structured data with retry logic
            extracted_data = await self.retry_manager.retry_with_backoff(
                self._extract_data_with_timeout, invoice_content
            )
            
            # Step 5: Post-process and validate
            extracted_data = self._post_process_extraction(extracted_data, pdf_path)
            
            # Step 6: Cache successful result
            if extracted_data.confidence_level != ConfidenceLevel.ERROR:
                self.cache.save_to_cache(pdf_path, extracted_data)
                self.processing_stats['successful_extractions'] += 1
            else:
                self.processing_stats['failed_extractions'] += 1
            
            # Step 7: Log result with timing
            total_time = time.time() - start_time
            self.processing_stats['total_processing_time'] += total_time
            
            confidence_icon = {
                ConfidenceLevel.HIGH: "🟢",
                ConfidenceLevel.MEDIUM: "🟡",
                ConfidenceLevel.LOW: "🟠",
                ConfidenceLevel.ERROR: "🔴"
            }.get(extracted_data.confidence_level, "❓")
            
            logger.info(f"{confidence_icon} {invoice_filename}: ${extracted_data.total_usd_amount} "
                       f"({extracted_data.confidence_level.value}, {total_time:.1f}s)")
            
            return extracted_data
            
        except Exception as e:
            # Handle errors gracefully
            total_time = time.time() - start_time
            self.processing_stats['failed_extractions'] += 1
            self.processing_stats['total_processing_time'] += total_time
            
            logger.error(f"❌ Error processing {invoice_filename} ({total_time:.1f}s): {str(e)}")
            
            return CommercialInvoiceData(
                invoice_number=f"ERROR_{Path(pdf_path).stem}",
                company_name="PROCESSING_ERROR",
                total_usd_amount=Decimal('0'),
                confidence_level=ConfidenceLevel.ERROR,
                extraction_notes=f"Processing failed: {str(e)[:200]}",
                currency="UNKNOWN"
            )
        
        finally:
            # Force garbage collection for large files
            if Path(pdf_path).stat().st_size > 5 * 1024 * 1024:  # > 5MB
                gc.collect()
    
    async def process_single_invoice_enhanced(self, pdf_path: str, esn: str) -> InvoiceExtractionResult:
        """Enhanced processing with line item extraction"""
        
        invoice_filename = Path(pdf_path).name
        start_time = time.time()
        
        try:
            logger.info(f"📄 Enhanced processing: {invoice_filename}")
            
            # Step 1: Parse PDF (same as before)
            docs = await self.retry_manager.retry_with_backoff(
                self._parse_pdf_with_timeout, pdf_path
            )
            
            if not docs:
                raise ValueError("No content extracted from PDF")
            
            # Step 2: Prepare content
            invoice_content = self._prepare_invoice_content(docs)
            
            # Step 3: Try enhanced extraction first
            enhanced_success = False
            enhanced_data = None
            
            try:
                enhanced_data = await self.retry_manager.retry_with_backoff(
                    self._extract_enhanced_data_with_timeout, invoice_content
                )
                enhanced_success = True
                logger.info(f"✅ Line items extracted: {len(enhanced_data.line_items)} items")
                
            except Exception as e:
                logger.warning(f"⚠️ Enhanced extraction failed: {e}")
                enhanced_success = False
            
            # Step 4: Fallback to legacy extraction if enhanced fails
            if not enhanced_success or not enhanced_data:
                logger.info("🔄 Falling back to legacy extraction")
                legacy_data = await self.retry_manager.retry_with_backoff(
                    self._extract_data_with_timeout, invoice_content
                )
                
                # Convert legacy to enhanced format
                enhanced_data = self._convert_legacy_to_enhanced(legacy_data)
            
            # Step 5: Create legacy compatibility data
            legacy_data = self._convert_enhanced_to_legacy(enhanced_data)
            
            # Step 6: Post-process both formats
            enhanced_data = self._post_process_enhanced_extraction(enhanced_data, pdf_path)
            legacy_data = self._post_process_extraction(legacy_data, pdf_path)
            
            # Step 7: Create complete result
            processing_time = time.time() - start_time
            
            result = InvoiceExtractionResult(
                enhanced_data=enhanced_data,
                legacy_data=legacy_data,
                processing_time=processing_time,
                extraction_method="enhanced" if enhanced_success else "legacy_fallback",
                line_item_extraction_success=enhanced_success and len(enhanced_data.line_items) > 0
            )
            
            # Step 8: Cache result (enhanced format)
            if enhanced_data.confidence_level != ConfidenceLevel.ERROR:
                self._save_enhanced_to_cache(pdf_path, result)
            
            logger.info(f"🎯 {invoice_filename}: ${enhanced_data.total_usd_amount} "
                       f"({len(enhanced_data.line_items)} items, {processing_time:.1f}s)")
            
            return result
            
        except Exception as e:
            # Fallback error handling
            processing_time = time.time() - start_time
            logger.error(f"❌ Enhanced processing failed for {invoice_filename}: {e}")
            
            # Create error result
            error_enhanced = EnhancedInvoiceData(
                invoice_number=f"ERROR_{Path(pdf_path).stem}",
                company_name="PROCESSING_ERROR",
                total_usd_amount=Decimal('0'),
                confidence_level=ConfidenceLevel.ERROR,
                extraction_notes=f"Enhanced processing failed: {str(e)[:200]}"
            )
            
            error_legacy = CommercialInvoiceData(
                invoice_number=f"ERROR_{Path(pdf_path).stem}",
                company_name="PROCESSING_ERROR", 
                total_usd_amount=Decimal('0'),
                confidence_level=ConfidenceLevel.ERROR,
                extraction_notes=f"Processing failed: {str(e)[:200]}"
            )
            
            return InvoiceExtractionResult(
                enhanced_data=error_enhanced,
                legacy_data=error_legacy,
                processing_time=processing_time,
                extraction_method="error",
                line_item_extraction_success=False
            )
    
    # ============================================
    # HELPER METHODS (All inside the class)
    # ============================================
    
    async def _parse_pdf_with_timeout(self, pdf_path: str):
        """Parse PDF with timeout handling"""
        try:
            # Set a reasonable timeout for PDF parsing
            return await asyncio.wait_for(
                self.parser.aload_data(pdf_path),
                timeout=120.0  # 2 minutes max for parsing
            )
        except asyncio.TimeoutError:
            raise ValueError(f"PDF parsing timeout for {Path(pdf_path).name}")
    
    def _prepare_invoice_content(self, docs) -> str:
        """Efficiently prepare invoice content for extraction"""
        # Combine pages efficiently
        content_parts = []
        for i, doc in enumerate(docs):
            page_content = doc.get_content()
            if page_content and len(page_content.strip()) > 10:
                content_parts.append(f"=== PAGE {i+1} ===\n{page_content}")
        
        full_content = "\n\n".join(content_parts)
        
        # Validate content length
        if len(full_content.strip()) < 10:
            raise ValueError("PDF content too short - possible parsing error")
        
        # Truncate extremely long content for speed (keep first 15k chars)
        if len(full_content) > 15000:
            full_content = full_content[:15000] + "\n\n[Content truncated for processing speed]"
            logger.debug("Content truncated for faster processing")
        
        return full_content
    
    async def _extract_data_with_timeout(self, invoice_content: str) -> CommercialInvoiceData:
        """Extract data with timeout handling"""
        try:
            return await asyncio.wait_for(
                self.llm.astructured_predict(
                    CommercialInvoiceData,
                    self.extraction_prompt,
                    invoice_content=invoice_content
                ),
                timeout=45.0  # 45 seconds max for extraction
            )
        except asyncio.TimeoutError:
            raise ValueError("AI extraction timeout")
    
    async def _extract_enhanced_data_with_timeout(self, invoice_content: str) -> EnhancedInvoiceData:
        """Extract enhanced data with line item separation"""
        
        # Enhanced prompt for line item extraction
        enhanced_prompt = ChatPromptTemplate.from_messages([
            ("system", """Extract detailed line item data from this Spanish commercial invoice.

FOR EACH INDIVIDUAL PRODUCT/SKU on the invoice, extract:
1. SKU/Reference number (REF CLIENTE)
2. Product description (DESCRIPCIÓN DEL MATERIAL)  
3. Individual quantity for that SKU
4. Individual unit price for that SKU
5. Individual line total for that SKU

ALSO EXTRACT invoice header information:
- Invoice number
- Company name  
- Invoice date/time
- Total invoice amount

IMPORTANT: 
- Separate each SKU into its own line item
- If multiple SKUs are listed together, split them
- Calculate individual line totals (quantity × unit_price)
- Ensure line totals sum to invoice total

RETURN structured data with separate line items, not combined strings."""),
            
            ("user", """Extract line items and header from this invoice:

REQUIRED OUTPUT STRUCTURE:
1. Invoice header (number, company, date, total amount)
2. Individual line items (each SKU separate)
3. Line item details (quantity, unit price, line total per SKU)

If multiple SKUs share the same total, estimate individual quantities proportionally.

Invoice content: {invoice_content}""")
        ])
        
        try:
            return await asyncio.wait_for(
                self.llm.astructured_predict(
                    EnhancedInvoiceData,
                    enhanced_prompt,
                    invoice_content=invoice_content
                ),
                timeout=60.0  # Longer timeout for complex extraction
            )
        except asyncio.TimeoutError:
            raise ValueError("Enhanced AI extraction timeout")
    
    def _post_process_extraction(self, extracted_data: CommercialInvoiceData, pdf_path: str) -> CommercialInvoiceData:
        """Post-process and validate extraction results"""
        
        # Validate amount
        if extracted_data.total_usd_amount <= 0:
            extracted_data.confidence_level = ConfidenceLevel.ERROR
            extracted_data.extraction_notes = "No valid positive amount extracted"
        elif extracted_data.total_usd_amount > 10000000:  # > $10M sanity check
            extracted_data.confidence_level = ConfidenceLevel.LOW
            extracted_data.extraction_notes = "Amount seems unusually large - please verify"
        
        # Ensure required fields
        if not extracted_data.invoice_number:
            extracted_data.invoice_number = f"EXTRACTED_{Path(pdf_path).stem}"
        
        if not extracted_data.company_name:
            extracted_data.company_name = "UNKNOWN_COMPANY"
        
        # Set default currency if missing
        if not extracted_data.currency:
            extracted_data.currency = "USD"
        
        return extracted_data
    
    def _post_process_enhanced_extraction(self, enhanced_data: EnhancedInvoiceData, pdf_path: str) -> EnhancedInvoiceData:
        """Post-process enhanced extraction result"""
        
        # Validate line items total vs invoice total
        if enhanced_data.line_items:
            line_items_sum = sum(item.line_total for item in enhanced_data.line_items)
            enhanced_data.line_items_total = line_items_sum
            enhanced_data.total_line_items = len(enhanced_data.line_items)
            
            # Check for significant discrepancy
            difference = abs(enhanced_data.total_usd_amount - line_items_sum)
            if difference > 0.01:  # More than 1 cent difference
                note = f"Line items total (${line_items_sum}) differs from invoice total (${enhanced_data.total_usd_amount}) by ${difference}"
                if enhanced_data.extraction_notes:
                    enhanced_data.extraction_notes += f" | {note}"
                else:
                    enhanced_data.extraction_notes = note
        
        return enhanced_data
    
    def _convert_legacy_to_enhanced(self, legacy_data: CommercialInvoiceData) -> EnhancedInvoiceData:
        """Convert legacy format to enhanced format"""
        
        # Split combined fields if they contain multiple items
        line_items = []
        
        if hasattr(legacy_data, 'client_reference') and legacy_data.client_reference:
            skus = [s.strip() for s in legacy_data.client_reference.split(',') if s.strip()]
            descriptions = []
            
            if hasattr(legacy_data, 'material_description') and legacy_data.material_description:
                descriptions = [d.strip() for d in legacy_data.material_description.split(',') if d.strip()]
            
            # Create line items from split data
            for i, sku in enumerate(skus):
                description = descriptions[i] if i < len(descriptions) else f"Product {i+1}"
                
                # Estimate quantities and unit prices
                total_qty = getattr(legacy_data, 'cantidad_total', 0) or 0
                unit_price = getattr(legacy_data, 'valor_unitario', 0) or 0
                
                # FIX: Ensure proper type conversion
                estimated_qty = float(total_qty) / len(skus) if len(skus) > 0 else 0
                estimated_total = legacy_data.total_usd_amount / len(skus) if len(skus) > 0 else legacy_data.total_usd_amount
                
                line_item = LineItem(
                    line_number=i + 1,
                    sku=sku,
                    description=description,
                    quantity=estimated_qty,
                    unit_price=Decimal(str(unit_price)),
                    line_total=estimated_total
                )
                line_items.append(line_item)
        
        # If no line items could be created, create a single item
        if not line_items and legacy_data.total_usd_amount > 0:
            line_item = LineItem(
                line_number=1,
                sku=getattr(legacy_data, 'client_reference', 'UNKNOWN_SKU') or 'UNKNOWN_SKU',
                description=getattr(legacy_data, 'material_description', 'Unknown Product') or 'Unknown Product',
                quantity=float(getattr(legacy_data, 'cantidad_total', 1) or 1),  # FIX: Convert to float
                unit_price=legacy_data.total_usd_amount,
                line_total=legacy_data.total_usd_amount
            )
            line_items.append(line_item)
        
        # Create enhanced data
        enhanced_data = EnhancedInvoiceData(
            invoice_number=legacy_data.invoice_number,
            company_name=legacy_data.company_name,
            total_usd_amount=legacy_data.total_usd_amount,
            currency=legacy_data.currency,
            confidence_level=legacy_data.confidence_level,
            extraction_notes=legacy_data.extraction_notes,
            amount_source_text=legacy_data.amount_source_text,
            line_items=line_items,
            total_line_items=len(line_items),
            line_items_total=sum(item.line_total for item in line_items),
            # Legacy compatibility
            client_reference=getattr(legacy_data, 'client_reference', None),
            material_description=getattr(legacy_data, 'material_description', None),
            cantidad_total=getattr(legacy_data, 'cantidad_total', None),
            valor_unitario=getattr(legacy_data, 'valor_unitario', None),
            fecha_hora=getattr(legacy_data, 'fecha_hora', None)
        )
        
        return enhanced_data

    def _convert_enhanced_to_legacy(self, enhanced_data: EnhancedInvoiceData) -> CommercialInvoiceData:
        """Convert enhanced format to legacy format for compatibility"""
        
        # Combine line items back to legacy format
        combined_skus = ", ".join([item.sku for item in enhanced_data.line_items])
        combined_descriptions = ", ".join([item.description for item in enhanced_data.line_items])
        total_quantity = sum(item.quantity for item in enhanced_data.line_items)
        
        # FIX: Convert Decimal to float for division
        avg_unit_price = (float(enhanced_data.total_usd_amount) / total_quantity) if total_quantity > 0 else 0
        
        legacy_data = CommercialInvoiceData(
            invoice_number=enhanced_data.invoice_number,
            company_name=enhanced_data.company_name,
            total_usd_amount=enhanced_data.total_usd_amount,
            currency=enhanced_data.currency,
            confidence_level=enhanced_data.confidence_level,
            extraction_notes=enhanced_data.extraction_notes,
            amount_source_text=enhanced_data.amount_source_text,
            client_reference=combined_skus or enhanced_data.client_reference,
            material_description=combined_descriptions or enhanced_data.material_description,
            cantidad_total=total_quantity or enhanced_data.cantidad_total,
            valor_unitario=float(avg_unit_price) or enhanced_data.valor_unitario,
            fecha_hora=enhanced_data.fecha_hora
        )
        
        return legacy_data

    def _save_enhanced_to_cache(self, file_path: str, result: InvoiceExtractionResult):
        """Save enhanced extraction result to cache"""
        try:
            file_hash = self.cache._get_file_hash(file_path)
            cache_path = self.cache.get_cache_path(file_hash)
            
            # Save enhanced result to cache
            cache_data = {
                'invoice_number': result.enhanced_data.invoice_number,
                'company_name': result.enhanced_data.company_name,
                'total_usd_amount': str(result.enhanced_data.total_usd_amount),
                'currency': result.enhanced_data.currency,
                'confidence_level': result.enhanced_data.confidence_level.value,
                'extraction_notes': result.enhanced_data.extraction_notes,
                'amount_source_text': result.enhanced_data.amount_source_text,
                'client_reference': result.enhanced_data.client_reference,
                'material_description': result.enhanced_data.material_description,
                'fecha_hora': result.enhanced_data.fecha_hora,
                'cantidad_total': result.enhanced_data.cantidad_total,
                'valor_unitario': result.enhanced_data.valor_unitario,
                # Enhanced fields
                'line_items': [
                    {
                        'line_number': item.line_number,
                        'sku': item.sku,
                        'description': item.description,
                        'quantity': item.quantity,
                        'unit_price': str(item.unit_price),
                        'line_total': str(item.line_total),
                        'unit_of_measure': item.unit_of_measure,
                        'country_of_origin': item.country_of_origin,
                        'hts_code': item.hts_code
                    }
                    for item in result.enhanced_data.line_items
                ],
                'processing_time': result.processing_time,
                'extraction_method': result.extraction_method,
                'line_item_extraction_success': result.line_item_extraction_success,
                'cached_at': time.time(),
                'file_path': str(file_path)
            }
            
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            logger.debug(f"Saved enhanced result to cache: {cache_path}")
            
        except Exception as e:
            logger.warning(f"Failed to save enhanced result to cache: {e}")

# Legacy compatibility - keep existing InvoiceProcessor class name
class InvoiceProcessor(OptimizedInvoiceProcessor):
    """Legacy compatibility class"""
    pass

# ============================================
# PERFORMANCE UTILITIES
# ============================================

class ProcessorBenchmark:
    """Benchmark utility for processor performance testing"""
    
    def __init__(self, processor: OptimizedInvoiceProcessor):
        self.processor = processor
    
    async def benchmark_single_file(self, pdf_path: str, iterations: int = 3) -> Dict[str, float]:
        """Benchmark single file processing"""
        times = []
        
        for i in range(iterations):
            start_time = time.time()
            result = await self.processor.process_single_invoice(pdf_path, "TEST")
            duration = time.time() - start_time
            times.append(duration)
            
            # Clear cache between iterations for accurate timing
            if hasattr(self.processor, 'cache'):
                cache_path = self.processor.cache.get_cache_path(
                    self.processor.cache._get_file_hash(pdf_path)
                )
                cache_path.unlink(missing_ok=True)
        
        return {
            'min_time': min(times),
            'max_time': max(times),
            'avg_time': sum(times) / len(times),
            'total_time': sum(times)
        }

# ============================================
# FACTORY FUNCTIONS
# ============================================

def create_optimized_processor(config: SystemConfig) -> OptimizedInvoiceProcessor:
    """Factory function to create optimized processor"""
    return OptimizedInvoiceProcessor(config)

def create_processor_with_custom_cache(config: SystemConfig, cache_dir: str, cache_size: int) -> OptimizedInvoiceProcessor:
    """Factory function to create processor with custom cache settings"""
    processor = OptimizedInvoiceProcessor(config)
    processor.cache = InvoiceCache(cache_dir, cache_size)
    return processor