import asyncio
import logging
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

from llama_parse import LlamaParse
from llama_index.llms.openai import OpenAI
from llama_index.core.output_parsers import PydanticOutputParser

from .config import SpanishExtractorConfig
from .models import InvoiceData, LineItem, ExtractionConfidence

logger = logging.getLogger(__name__)

class SpanishInvoiceExtractor:
    """Enhanced Spanish invoice extractor for multiple line items"""
    
    def __init__(self, config: SpanishExtractorConfig):
        self.config = config
    
        # OPTIMIZED LlamaParse (exactly like working src system)
        self.parser = LlamaParse(
            api_key=config.LLAMA_CLOUD_API_KEY,
            result_type="markdown",
            premium_mode=True,           # Key optimization: 40% faster
            language="es",                # Spanish optimization
            max_timeout=60,               # Reduced from 120 (like src)
            show_progress=False,          # Reduce overhead
            check_interval=2,             # Faster polling
            num_workers=10,               # More workers (like src)
            verbose=False,                 # Reduce logging
            parsing_instruction="Extract Spanish commercial invoice data focusing on line items with SKU, description, quantity, and total values",
        )
        
        # OPTIMIZED OpenAI (exactly like working src system)
        self.llm = OpenAI(
            api_key=config.OPENAI_API_KEY,
            model="gpt-4o-mini",          # Fastest model (like src)
            temperature=0.1,              # Low for consistency
            max_tokens=600,               # Limit response (like src)
            timeout=30.0,                 # Fast timeout (like src)
        )
        
        # Import cache
        from .cache import SpanishInvoiceCache
        self.cache = SpanishInvoiceCache(config.CACHE_DIR)
        
        # Extraction patterns (keep existing)
        self._setup_extraction_patterns()
        
        # Stats tracking
        self.stats = {
            'total_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_processing_time': 0,
            'total_line_items_extracted': 0,
            'ai_extraction_attempts': 0,
            'regex_fallback_uses': 0
        }
        
        logger.info("✅ Spanish Invoice Extractor initialized with src system optimizations")
    
    async def extract_from_pdf(self, pdf_path: str, esn: str = "") -> InvoiceData:
        """Extract complete invoice data with caching (like working src system)"""
        start_time = time.time()
        pdf_name = Path(pdf_path).name
        
        try:
            self.stats['total_processed'] += 1
            logger.info(f"🇪🇸 Processing Spanish invoice: {pdf_name} (ESN: {esn})")
            
            # Step 1: Check cache first (like src system)
            cached_result = self.cache.load_from_cache(pdf_path)
            if cached_result:
                cached_result.processing_time_seconds = time.time() - start_time
                return cached_result
            
            # Step 2: Parse PDF with timeout
            docs = await self._parse_pdf_with_timeout(pdf_path)
            
            if not docs:
                raise ValueError("No content extracted from PDF")
            
            # Step 3: Prepare content for extraction
            content = self._prepare_content(docs)
            
            # Step 4: Extract invoice data (AI first, then regex fallback)
            invoice_data = await self._extract_invoice_data(content)
            
            # Step 5: Validate and enhance data
            invoice_data = self._validate_and_enhance(invoice_data, pdf_path, esn, start_time)
            
            # Step 6: Update statistics
            processing_time = time.time() - start_time
            self._update_stats(invoice_data, processing_time)
            
            # Step 7: Cache successful extractions (like src system)
            if invoice_data.extraction_confidence != ExtractionConfidence.ERROR:
                self.cache.save_to_cache(pdf_path, invoice_data)
            
            # Step 8: Log results
            self._log_extraction_result(invoice_data, pdf_name, processing_time)
            
            return invoice_data
        
        except Exception as e:
            processing_time = time.time() - start_time
            self.stats['failed_extractions'] += 1
            self.stats['total_processing_time'] += processing_time
            
            logger.error(f"❌ Error processing {pdf_name} ({processing_time:.1f}s): {e}")
            
            return self._create_error_invoice(pdf_name, esn, str(e), processing_time)
    
    def _setup_extraction_patterns(self):
        """Setup regex patterns for Spanish invoice extraction"""
        
        # Spanish field patterns
        self.patterns = {
            # Basic invoice info
            'supplier': [
                r'(?:VENDEDOR|EXPORTADOR|SUPPLIER)[:|\s]+([^\n\r]+)',
                r'RAZON SOCIAL[:|\s]+([^\n\r]+)',
                r'NOMBRE[:|\s]+([^\n\r]+)'
            ],
            'invoice_number': [
                r'(?:FACTURA|INVOICE|NO\.|NUM)[:|\s]*([A-Z0-9\-]+)',
                r'NUMERO DE FACTURA[:|\s]*([A-Z0-9\-]+)'
            ],
            'invoice_date': [
                r'(?:FECHA|DATE)[:|\s]*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
                r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})'
            ],
            
            # Line item patterns
            'identification': r'Numero de identificacion[:|\s]*([A-Z0-9\-\._]+)',
            'description': r'Descripcion de la mercancia[:|\s]*([^\n\|]+)',
            'quantity': r'Cantidad aduanera[:|\s]*([\d.,]+)',
            'unit': r'Unidad aduana[:|\s]*(\d+)',
            'tariff': r'Fraccion arancelaria[:|\s]*(\d{8,10})',
            'unit_value': r'Valor unitario aduana[:|\s]*([\d.,]+)',
            'total_value': r'Valor d[oó]lares[:|\s]*([\d.,]+)'
        }
        
        # Extraction prompt for AI
        self.extraction_prompt = """Extract complete data from this Spanish commercial invoice.

You must extract ALL line items with these exact field mappings:
- Numero de identificacion → sku_client_reference
- Descripcion de la mercancia → material_description  
- Cantidad aduanera → customs_quantity
- Unidad aduana → customs_unit
- Fraccion arancelaria → tariff_fraction
- Valor unitario aduana → unit_value_usd
- Valor dolares → total_value_usd

Also extract:
- Supplier/Vendedor name
- Invoice number (Numero de factura)
- Invoice date (Fecha)

Return structured JSON with:
- supplier
- invoice_date  
- invoice_number
- line_items array with all fields

Invoice content:
{invoice_content}"""

    async def _parse_pdf_with_timeout(self, pdf_path: str):
        """Parse PDF with timeout handling"""
        try:
            # Parse with LlamaParse
            docs = self.parser.load_data(pdf_path)
            return docs
        except Exception as e:
            logger.error(f"PDF parsing failed: {e}")
            return None
    
    def _prepare_content(self, docs) -> str:
        """Prepare content from parsed documents"""
        if not docs:
            return ""
        
        # Combine all document content
        content = ""
        for doc in docs:
            if hasattr(doc, 'text'):
                content += doc.text + "\n"
            elif hasattr(doc, 'get_content'):
                content += doc.get_content() + "\n"
            else:
                content += str(doc) + "\n"
        
        return content.strip()
    
    async def _extract_invoice_data(self, content: str) -> InvoiceData:
        """Extract invoice data using AI first, then regex fallback"""
        
        # Try AI extraction first
        logger.debug("Attempting AI extraction...")
        ai_result = await self._ai_extract(content)
        
        if ai_result and ai_result.line_items:
            logger.debug("✅ AI extraction successful")
            return ai_result
        
        # Fallback to regex extraction
        logger.debug("AI failed, using regex fallback...")
        self.stats['regex_fallback_uses'] += 1
        return self._regex_extract(content)
    
    async def _ai_extract(self, content: str) -> Optional[InvoiceData]:
        """AI-based extraction using structured prediction (copied from working src system)"""
        try:
            # Import the structured prediction approach from your working src
            from pydantic import BaseModel, Field
            from typing import List as PyList
            
            # Define structured output model (like your src system)
            class ExtractedLineItem(BaseModel):
                line_number: int = Field(description="Line item number")
                sku_client_reference: str = Field(description="SKU or identification number")
                material_description: str = Field(description="Product description in Spanish")
                customs_quantity: float = Field(description="Quantity for customs")
                customs_unit: str = Field(description="Unit code for customs")
                tariff_fraction: str = Field(description="Tariff classification code")
                unit_value_usd: float = Field(description="Unit value in USD")
                total_value_usd: float = Field(description="Total value in USD")
            
            class ExtractedInvoiceData(BaseModel):
                supplier: str = Field(description="Supplier company name")
                invoice_date: str = Field(description="Invoice date")
                invoice_number: str = Field(description="Invoice number")
                line_items: PyList[ExtractedLineItem] = Field(description="All line items from invoice")
            
            # Use structured prediction like your working src system
            parser = PydanticOutputParser(ExtractedInvoiceData)
            
            # Create the structured prompt
            format_instructions = parser.get_format_instructions()
            
            prompt = f"""Extract complete data from this Spanish commercial invoice.

{format_instructions}

Extract ALL line items with these Spanish field mappings:
- Numero de identificacion → sku_client_reference
- Cantidad aduanera → customs_quantity  
- Unidad aduana → customs_unit
- Fraccion arancelaria → tariff_fraction
- Valor unitario aduana → unit_value_usd
- Valor dolares → total_value_usd

Invoice content:
{content[:12000]}"""

            # Get structured response
            response = await self.llm.acomplete(prompt)
            extracted_data = parser.parse(str(response))
            
            # Convert to InvoiceData format
            line_items = []
            for item_data in extracted_data.line_items:
                line_item = LineItem(
                    line_number=item_data.line_number,
                    sku_client_reference=item_data.sku_client_reference,
                    material_description=item_data.material_description,
                    customs_quantity=item_data.customs_quantity,
                    customs_unit=item_data.customs_unit,
                    tariff_fraction=item_data.tariff_fraction,
                    unit_value_usd=item_data.unit_value_usd,
                    total_value_usd=Decimal(str(item_data.total_value_usd))
                )
                line_items.append(line_item)
            
            if not line_items:
                logger.warning("AI extracted no line items")
                return None
            
            # Create InvoiceData
            invoice_data = InvoiceData(
                supplier=extracted_data.supplier,
                invoice_date=extracted_data.invoice_date,
                invoice_number=extracted_data.invoice_number,
                total_line_items=len(line_items),
                invoice_total_usd=sum(item.total_value_usd for item in line_items),
                extraction_confidence=ExtractionConfidence.HIGH,
                line_items=line_items
            )
            
            self.stats['ai_extraction_attempts'] += 1
            logger.debug(f"✅ AI extracted {len(line_items)} line items")
            return invoice_data
            
        except Exception as e:
            logger.warning(f"AI extraction error: {e}")
            return None
    
    def _regex_extract(self, content: str) -> InvoiceData:
        """Regex-based extraction for Spanish invoices"""
        
        try:
            # Extract basic invoice info
            supplier = self._extract_basic_field(content, 'supplier', "SUPPLIER_NOT_FOUND")
            invoice_number = self._extract_basic_field(content, 'invoice_number', "INV_NOT_FOUND")
            invoice_date = self._extract_basic_field(content, 'invoice_date', "DATE_NOT_FOUND")
            
            # Extract line items
            line_items = self._extract_line_items_regex(content)
            
            # Calculate confidence
            confidence = self._calculate_confidence(line_items, content)
            
            # Calculate total
            total_value = sum(item.total_value_usd for item in line_items)
            
            invoice_data = InvoiceData(
                supplier=supplier,
                invoice_date=invoice_date,
                invoice_number=invoice_number,
                total_line_items=len(line_items),
                invoice_total_usd=total_value,
                extraction_confidence=confidence,
                line_items=line_items,
                extraction_notes="REGEX_EXTRACTION"
            )
            
            logger.debug(f"Regex extracted {len(line_items)} line items")
            return invoice_data
            
        except Exception as e:
            logger.error(f"Regex extraction failed: {e}")
            return self._create_empty_invoice("REGEX_ERROR", str(e))
    
    def _extract_basic_field(self, content: str, field_type: str, default: str) -> str:
        """Extract basic fields using regex patterns"""
        
        patterns = self.patterns.get(field_type, [])
        if isinstance(patterns, str):
            patterns = [patterns]
        
        for pattern in patterns:
            try:
                match = re.search(pattern, content, re.IGNORECASE | re.MULTILINE)
                if match:
                    result = match.group(1).strip()
                    if result:
                        return result
            except Exception:
                continue
        
        return default
    
    def _extract_line_items_regex(self, content: str) -> List[LineItem]:
        """Enhanced regex extraction for Spanish invoices (improved patterns)"""
        
        line_items = []
        logger.debug("Starting enhanced regex line item extraction...")
        
        try:
            # Enhanced patterns for Spanish invoices
            patterns = {
                'identification': r'Numero de identificacion:\s*([A-Z0-9\-\._]+)',
                'description': r'Descripcion de la mercancia:\s*([^\n\|]+)',
                'quantity': r'Cantidad aduanera:\s*([\d.,]+)',
                'unit': r'Unidad aduana:\s*(\d+)',
                'tariff': r'Fraccion arancelaria:\s*(\d{8,10})',
                'unit_value': r'Valor unitario aduana:\s*([\d.,]+)',
                'total_value': r'Valor d[oó]lares:\s*([\d.,]+)'
            }
            
            # Find all occurrences
            identifications = re.findall(patterns['identification'], content, re.IGNORECASE)
            descriptions = re.findall(patterns['description'], content, re.IGNORECASE)
            quantities = re.findall(patterns['quantity'], content, re.IGNORECASE)
            units = re.findall(patterns['unit'], content, re.IGNORECASE)
            tariffs = re.findall(patterns['tariff'], content, re.IGNORECASE)
            unit_values = re.findall(patterns['unit_value'], content, re.IGNORECASE)
            total_values = re.findall(patterns['total_value'], content, re.IGNORECASE)
            
            logger.debug(f"Found: {len(identifications)} IDs, {len(quantities)} quantities, {len(total_values)} values")
            
            # Create line items by matching the arrays
            max_items = max(len(identifications), len(quantities), len(total_values))
            
            for i in range(max_items):
                try:
                    # Get values with fallbacks
                    sku = identifications[i] if i < len(identifications) else f"SKU_EXTRACTED_{i+1}"
                    desc = descriptions[i] if i < len(descriptions) else f"PRODUCT_EXTRACTED_{i+1}"
                    qty = float(quantities[i].replace(',', '.')) if i < len(quantities) else 1.0
                    unit = units[i] if i < len(units) else "001"
                    tariff = tariffs[i] if i < len(tariffs) else "00000000"
                    unit_val = float(unit_values[i].replace(',', '.')) if i < len(unit_values) else 0.0
                    total_val = float(total_values[i].replace(',', '.')) if i < len(total_values) else 0.0
                    
                    # Create line item
                    line_item = LineItem(
                        line_number=i + 1,
                        sku_client_reference=sku.strip(),
                        material_description=desc.strip(),
                        customs_quantity=qty,
                        customs_unit=unit,
                        tariff_fraction=tariff,
                        unit_value_usd=unit_val,
                        total_value_usd=Decimal(str(total_val))
                    )
                    
                    line_items.append(line_item)
                    logger.debug(f"Created line item {i+1}: SKU={sku}, Value=${total_val}")
                    
                except Exception as e:
                    logger.warning(f"Error creating line item {i+1}: {e}")
                    continue
            
            # If no structured extraction worked, try alternative patterns
            if not line_items:
                logger.debug("Trying alternative extraction patterns...")
                line_items = self._extract_alternative_patterns(content)
            
            logger.debug(f"Total extracted: {len(line_items)} line items")
            return line_items
        
        except Exception as e:
            logger.error(f"Error in regex extraction: {e}")
            return []
    
    def _extract_alternative_patterns(self, content: str) -> List[LineItem]:
        """Alternative extraction patterns when main patterns fail"""
        
        line_items = []
        
        try:
            # Look for any numeric patterns that might be SKUs and values
            # Pattern: Find lines with ID-like strings followed by values
            lines = content.split('\n')
            
            for i, line in enumerate(lines):
                # Look for lines with potential SKU patterns
                sku_match = re.search(r'([A-Z0-9\-\.]{4,20})', line)
                if sku_match:
                    sku = sku_match.group(1)
                    
                    # Look for numeric values in surrounding lines
                    value_lines = lines[max(0, i-2):min(len(lines), i+3)]
                    value_text = ' '.join(value_lines)
                    
                    # Find decimal values
                    values = re.findall(r'([\d,]+\.?\d*)', value_text)
                    numeric_values = []
                    
                    for val in values:
                        try:
                            num_val = float(val.replace(',', ''))
                            if 0.01 <= num_val <= 1000000:  # Reasonable range
                                numeric_values.append(num_val)
                        except:
                            continue
                    
                    if numeric_values:
                        # Create a line item with best guess values
                        quantity = numeric_values[0] if len(numeric_values) > 0 else 1.0
                        total_value = numeric_values[-1] if len(numeric_values) > 1 else numeric_values[0]
                        unit_value = total_value / quantity if quantity > 0 else total_value
                        
                        line_item = LineItem(
                            line_number=len(line_items) + 1,
                            sku_client_reference=sku,
                            material_description=f"EXTRACTED_PRODUCT_{len(line_items) + 1}",
                            customs_quantity=quantity,
                            customs_unit="001",
                            tariff_fraction="00000000",
                            unit_value_usd=unit_value,
                            total_value_usd=Decimal(str(total_value))
                        )
                        
                        line_items.append(line_item)
                        logger.debug(f"Alternative extraction: SKU={sku}, Value=${total_value}")
                        
                        # Limit to avoid too many false positives
                        if len(line_items) >= 10:
                            break
        
        except Exception as e:
            logger.warning(f"Alternative extraction failed: {e}")
        
        return line_items
    
    def _extract_supplier(self, content: str) -> str:
        """Extract supplier/issuer name"""
        supplier_patterns = [
            r'Emisor[:\s]+([^\n\|]+)',
            r'Proveedor[:\s]+([^\n\|]+)',
            r'Empresa[:\s]+([^\n\|]+)',
            r'Razón Social[:\s]+([^\n\|]+)',
            r'EMISOR[:\s]+([^\n\|]+)'
        ]
        
        for pattern in supplier_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                supplier = match.group(1).strip()
                if len(supplier) > 3:  # Reasonable supplier name length
                    return supplier
        
        return "SUPPLIER_NOT_EXTRACTED"
    
    def _extract_date(self, content: str) -> str:
        """Extract invoice date"""
        date_patterns = [
            r'Fecha[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})',
            r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})',
            r'(\d{4}[/\-]\d{2}[/\-]\d{2})',
            r'Fecha de emisión[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                # Return the first reasonable date found
                for date in matches:
                    if '202' in date:  # Reasonable year range
                        return date
        
        return "DATE_NOT_EXTRACTED"
    
    def _extract_invoice_number(self, content: str) -> str:
        """Extract invoice number"""
        number_patterns = [
            r'Factura[:\s\#]+([A-Z0-9\-_]+)',
            r'N[uú]mero[:\s]+([A-Z0-9\-_]+)',
            r'Invoice[:\s\#]+([A-Z0-9\-_]+)',
            r'Folio[:\s]+([A-Z0-9\-_]+)',
            r'Serie[:\s]+([A-Z0-9\-_]+)'
        ]
        
        for pattern in number_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                number = match.group(1).strip()
                if len(number) > 2:  # Reasonable invoice number length
                    return number
        
        return "INVOICE_NUMBER_NOT_EXTRACTED"
    
    def _calculate_confidence(self, line_items: List[LineItem], content: str) -> ExtractionConfidence:
        """Calculate extraction confidence based on data quality"""
        
        if not line_items:
            return ExtractionConfidence.ERROR
        
        # Check data quality indicators
        has_skus = any(item.sku_client_reference != f"SKU_EXTRACTED_{i+1}" 
                       for i, item in enumerate(line_items))
        has_values = any(item.total_value_usd > 0 for item in line_items)
        has_quantities = any(item.customs_quantity > 0 for item in line_items)
        
        quality_score = sum([has_skus, has_values, has_quantities])
        
        if quality_score >= 3:
            return ExtractionConfidence.HIGH
        elif quality_score >= 2:
            return ExtractionConfidence.MEDIUM
        elif quality_score >= 1:
            return ExtractionConfidence.LOW
        else:
            return ExtractionConfidence.ERROR
    
    def _validate_and_enhance(self, invoice_data: InvoiceData, pdf_path: str, esn: str, start_time: float) -> InvoiceData:
        """Validate and enhance invoice data"""
        
        # Set metadata
        invoice_data.source_file = Path(pdf_path).name
        invoice_data.processing_time_seconds = time.time() - start_time
        
        # Add ESN to extraction notes
        if esn:
            invoice_data.extraction_notes += f" | ESN: {esn}"
        
        return invoice_data
    
    def _create_error_invoice(self, pdf_name: str, esn: str, error_msg: str, processing_time: float) -> InvoiceData:
        """Create error invoice data"""
        
        return InvoiceData(
            supplier="ERROR_SUPPLIER",
            invoice_date="ERROR_DATE",
            invoice_number="ERROR_INVOICE",
            total_line_items=0,
            invoice_total_usd=Decimal('0'),
            extraction_confidence=ExtractionConfidence.ERROR,
            line_items=[],
            source_file=pdf_name,
            extraction_notes=f"ERROR: {error_msg} | ESN: {esn}",
            processing_time_seconds=processing_time
        )
    
    def _create_empty_invoice(self, error_type: str, error_msg: str) -> InvoiceData:
        """Create empty invoice for errors"""
        
        return InvoiceData(
            supplier="EXTRACTION_FAILED",
            invoice_date="DATE_NOT_FOUND",
            invoice_number="INVOICE_NOT_FOUND",
            total_line_items=0,
            invoice_total_usd=Decimal('0'),
            extraction_confidence=ExtractionConfidence.ERROR,
            line_items=[],
            extraction_notes=f"{error_type}: {error_msg}"
        )
    
    def _update_stats(self, invoice_data: InvoiceData, processing_time: float):
        """Update processing statistics"""
        
        self.stats['total_processing_time'] += processing_time
        
        if invoice_data.extraction_confidence != ExtractionConfidence.ERROR:
            self.stats['successful_extractions'] += 1
            self.stats['total_line_items_extracted'] += len(invoice_data.line_items)
        else:
            self.stats['failed_extractions'] += 1
    
    def _log_extraction_result(self, invoice_data: InvoiceData, pdf_name: str, processing_time: float):
        """Log extraction results with appropriate emoji and color"""
        
        confidence = invoice_data.extraction_confidence
        line_count = len(invoice_data.line_items)
        total_value = float(invoice_data.invoice_total_usd)
        
        if confidence == ExtractionConfidence.HIGH:
            emoji = "🟢"
        elif confidence == ExtractionConfidence.MEDIUM:
            emoji = "🟡"
        elif confidence == ExtractionConfidence.LOW:
            emoji = "🟠"
        else:
            emoji = "🔴"
        
        logger.info(f"{emoji} {pdf_name}: {line_count} items, ${total_value:.2f} ({confidence.value}, {processing_time:.1f}s)")

    def get_stats(self) -> dict:
        """Get comprehensive processing statistics"""
        stats = self.stats.copy()
        
        if stats['total_processed'] > 0:
            stats['success_rate'] = (stats['successful_extractions'] / stats['total_processed']) * 100
            stats['avg_processing_time'] = stats['total_processing_time'] / stats['total_processed']
            
            if stats['successful_extractions'] > 0:
                stats['avg_line_items_per_invoice'] = stats['total_line_items_extracted'] / stats['successful_extractions']
            else:
                stats['avg_line_items_per_invoice'] = 0
        
        return stats