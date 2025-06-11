import hashlib
import json
import logging
from pathlib import Path
from typing import Optional
from .models import InvoiceData, LineItem, ExtractionConfidence
from decimal import Decimal

logger = logging.getLogger(__name__)

class SpanishInvoiceCache:
    """Caching system for Spanish invoice processing (copied from working src)"""
    
    def __init__(self, cache_dir: str = "data/spanish_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache initialized: {self.cache_dir}")
    
    def _get_file_hash(self, pdf_path: str) -> str:
        """Generate hash for PDF file content"""
        try:
            with open(pdf_path, 'rb') as f:
                # Read file in chunks for large files
                hasher = hashlib.md5()
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
                return hasher.hexdigest()
        except Exception as e:
            logger.warning(f"Could not hash file {pdf_path}: {e}")
            return ""
    
    def get_cache_path(self, file_hash: str) -> Path:
        """Get path for cached file"""
        return self.cache_dir / f"{file_hash}.json"
    
    def load_from_cache(self, pdf_path: str) -> Optional[InvoiceData]:
        """Load cached extraction result"""
        try:
            file_hash = self._get_file_hash(pdf_path)
            if not file_hash:
                return None
            
            cache_file = self.get_cache_path(file_hash)
            
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert back to InvoiceData
                line_items = []
                for item_data in data.get('line_items', []):
                    line_item = LineItem(
                        line_number=item_data['line_number'],
                        sku_client_reference=item_data['sku_client_reference'],
                        material_description=item_data['material_description'],
                        customs_quantity=item_data['customs_quantity'],
                        customs_unit=item_data['customs_unit'],
                        tariff_fraction=item_data['tariff_fraction'],
                        unit_value_usd=item_data['unit_value_usd'],
                        total_value_usd=Decimal(str(item_data['total_value_usd']))
                    )
                    line_items.append(line_item)
                
                invoice_data = InvoiceData(
                    supplier=data['supplier'],
                    invoice_date=data['invoice_date'],
                    invoice_number=data['invoice_number'],
                    total_line_items=data['total_line_items'],
                    invoice_total_usd=Decimal(str(data['invoice_total_usd'])),
                    extraction_confidence=ExtractionConfidence(data['extraction_confidence']),
                    line_items=line_items,
                    extraction_notes=f"CACHED: {data.get('extraction_notes', '')}"
                )
                
                logger.info(f"🟡 CACHED: {Path(pdf_path).name}")
                return invoice_data
                
        except Exception as e:
            logger.debug(f"Cache load failed for {Path(pdf_path).name}: {e}")
        
        return None
    
    def save_to_cache(self, pdf_path: str, invoice_data: InvoiceData):
        """Save extraction result to cache"""
        try:
            # Only cache successful extractions
            if invoice_data.extraction_confidence == ExtractionConfidence.ERROR:
                return
            
            file_hash = self._get_file_hash(pdf_path)
            if not file_hash:
                return
            
            cache_file = self.get_cache_path(file_hash)
            
            # Convert to JSON-serializable dict
            cache_data = {
                'supplier': invoice_data.supplier,
                'invoice_date': invoice_data.invoice_date,
                'invoice_number': invoice_data.invoice_number,
                'total_line_items': invoice_data.total_line_items,
                'invoice_total_usd': float(invoice_data.invoice_total_usd),
                'extraction_confidence': invoice_data.extraction_confidence.value,
                'line_items': [
                    {
                        'line_number': item.line_number,
                        'sku_client_reference': item.sku_client_reference,
                        'material_description': item.material_description,
                        'customs_quantity': item.customs_quantity,
                        'customs_unit': item.customs_unit,
                        'tariff_fraction': item.tariff_fraction,
                        'unit_value_usd': item.unit_value_usd,
                        'total_value_usd': float(item.total_value_usd)
                    }
                    for item in invoice_data.line_items
                ],
                'extraction_notes': invoice_data.extraction_notes,
                'cached_at': str(Path(pdf_path).name)
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"💾 Saved to cache: {Path(pdf_path).name}")
            
        except Exception as e:
            logger.debug(f"Cache save failed for {Path(pdf_path).name}: {e}")