from dataclasses import dataclass, field
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
from enum import Enum
import json

class ExtractionConfidence(Enum):
    """Confidence levels for extraction quality"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM" 
    LOW = "LOW"
    ERROR = "ERROR"

@dataclass
class LineItem:
    """Individual line item from Spanish invoice"""
    line_number: int
    sku_client_reference: str                # Numero de identificacion
    material_description: str                # Product description
    customs_quantity: float                  # Cantidad aduanera
    customs_unit: str                       # Unidad aduana
    tariff_fraction: str                    # Fraccion arancelaria
    unit_value_usd: float                   # Valor unitario aduana
    total_value_usd: Decimal                # Valor dolares
    
    def __post_init__(self):
        """Clean and validate line item data"""
        if not isinstance(self.total_value_usd, Decimal):
            self.total_value_usd = Decimal(str(self.total_value_usd))
        
        # Clean strings
        self.sku_client_reference = str(self.sku_client_reference).strip()
        self.material_description = str(self.material_description).strip()
        self.customs_unit = str(self.customs_unit).strip()
        self.tariff_fraction = str(self.tariff_fraction).strip()
        
        # Ensure numeric values are positive
        if self.customs_quantity < 0:
            self.customs_quantity = 0
        if self.unit_value_usd < 0:
            self.unit_value_usd = 0
        if self.total_value_usd < 0:
            self.total_value_usd = Decimal('0')

@dataclass
class InvoiceData:
    """Complete invoice with all line items"""
    # Invoice metadata
    supplier: str
    invoice_date: str
    invoice_number: str
    total_line_items: int
    invoice_total_usd: Decimal
    extraction_confidence: ExtractionConfidence
    
    # Line items
    line_items: List[LineItem] = field(default_factory=list)
    
    # Processing metadata
    source_file: str = ""
    extraction_notes: str = ""
    processing_time_seconds: float = 0.0
    
    def __post_init__(self):
        """Validate invoice data"""
        if not isinstance(self.invoice_total_usd, Decimal):
            self.invoice_total_usd = Decimal(str(self.invoice_total_usd))
        
        # Clean strings
        self.supplier = str(self.supplier).strip()
        self.invoice_date = str(self.invoice_date).strip()
        self.invoice_number = str(self.invoice_number).strip()
        self.source_file = str(self.source_file).strip()
        self.extraction_notes = str(self.extraction_notes).strip()
        
        # Update total line items count
        self.total_line_items = len(self.line_items)
    
    def get_total_calculated(self) -> Decimal:
        """Calculate total from line items"""
        return sum(item.total_value_usd for item in self.line_items)
    
    def validate_totals(self, tolerance: float = 0.01) -> bool:
        """Check if invoice total matches sum of line items"""
        calculated = self.get_total_calculated()
        tolerance_decimal = Decimal(str(tolerance))
        return abs(self.invoice_total_usd - calculated) <= tolerance_decimal
    
    def get_unique_skus(self) -> List[str]:
        """Get list of unique SKUs in this invoice"""
        return list(set(item.sku_client_reference for item in self.line_items))
    
    def get_skus_by_tariff(self) -> Dict[str, List[str]]:
        """Group SKUs by tariff fraction"""
        tariff_groups = {}
        for item in self.line_items:
            tariff = item.tariff_fraction
            if tariff not in tariff_groups:
                tariff_groups[tariff] = []
            tariff_groups[tariff].append(item.sku_client_reference)
        return tariff_groups

@dataclass
class ESNData:
    """Complete ESN with all invoices"""
    esn: str
    total_invoices: int
    total_line_items: int
    total_declared_value_usd: Decimal
    processing_status: str
    
    # All invoices in this ESN
    invoices: Dict[str, InvoiceData] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate ESN data"""
        if not isinstance(self.total_declared_value_usd, Decimal):
            self.total_declared_value_usd = Decimal(str(self.total_declared_value_usd))
        
        self.esn = str(self.esn).strip()
    
    def calculate_totals(self):
        """Recalculate totals from invoices"""
        self.total_invoices = len(self.invoices)
        self.total_line_items = sum(invoice.total_line_items for invoice in self.invoices.values())
        self.total_declared_value_usd = sum(invoice.invoice_total_usd for invoice in self.invoices.values())
    
    def get_success_rate(self) -> float:
        """Calculate success rate for this ESN"""
        if not self.invoices:
            return 0.0
        
        successful = len([inv for inv in self.invoices.values() 
                         if inv.extraction_confidence != ExtractionConfidence.ERROR])
        return (successful / len(self.invoices)) * 100
    
    def get_confidence_distribution(self) -> Dict[str, int]:
        """Get distribution of confidence levels"""
        distribution = {conf.value: 0 for conf in ExtractionConfidence}
        for invoice in self.invoices.values():
            distribution[invoice.extraction_confidence.value] += 1
        return distribution
    
    def get_unique_suppliers(self) -> List[str]:
        """Get list of unique suppliers for this ESN"""
        return list(set(invoice.supplier for invoice in self.invoices.values()))
    
    def get_date_range(self) -> tuple:
        """Get date range of invoices in this ESN"""
        dates = [invoice.invoice_date for invoice in self.invoices.values() 
                if invoice.invoice_date != "DATE_NOT_FOUND"]
        if not dates:
            return None, None
        return min(dates), max(dates)

@dataclass
class ExtractionSummary:
    """Overall extraction summary for all ESNs"""
    timestamp: str
    total_esns: int
    total_invoices: int
    total_line_items: int
    processing_time_seconds: float
    success_rate_percentage: float
    
    # All ESN data
    esn_data: Dict[str, ESNData] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize summary data"""
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()
    
    def calculate_summary_stats(self):
        """Calculate summary statistics from ESN data"""
        if not self.esn_data:
            return
        
        self.total_esns = len(self.esn_data)
        self.total_invoices = sum(esn.total_invoices for esn in self.esn_data.values())
        self.total_line_items = sum(esn.total_line_items for esn in self.esn_data.values())
        
        # Calculate overall success rate
        total_invoices_processed = sum(len(esn.invoices) for esn in self.esn_data.values())
        if total_invoices_processed > 0:
            successful_invoices = sum(
                len([inv for inv in esn.invoices.values() 
                    if inv.extraction_confidence != ExtractionConfidence.ERROR])
                for esn in self.esn_data.values()
            )
            self.success_rate_percentage = (successful_invoices / total_invoices_processed) * 100
        else:
            self.success_rate_percentage = 0.0
    
    def get_total_declared_value(self) -> Decimal:
        """Get total declared value across all ESNs"""
        return sum(esn.total_declared_value_usd for esn in self.esn_data.values())
    
    def get_processing_stats(self) -> Dict[str, float]:
        """Get detailed processing statistics"""
        all_invoices = []
        for esn_data in self.esn_data.values():
            all_invoices.extend(esn_data.invoices.values())
        
        if not all_invoices:
            return {}
        
        processing_times = [inv.processing_time_seconds for inv in all_invoices if inv.processing_time_seconds > 0]
        
        return {
            'avg_processing_time': sum(processing_times) / len(processing_times) if processing_times else 0,
            'max_processing_time': max(processing_times) if processing_times else 0,
            'min_processing_time': min(processing_times) if processing_times else 0,
            'total_processing_time': sum(processing_times) if processing_times else 0
        }
    
    def get_confidence_summary(self) -> Dict[str, Dict[str, int]]:
        """Get confidence level summary across all ESNs"""
        overall_distribution = {conf.value: 0 for conf in ExtractionConfidence}
        esn_distributions = {}
        
        for esn_name, esn_data in self.esn_data.items():
            esn_distribution = esn_data.get_confidence_distribution()
            esn_distributions[esn_name] = esn_distribution
            
            # Add to overall distribution
            for conf, count in esn_distribution.items():
                overall_distribution[conf] += count
        
        return {
            'overall': overall_distribution,
            'by_esn': esn_distributions
        }
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON export"""
        return {
            "extraction_summary": {
                "timestamp": self.timestamp,
                "total_esns": self.total_esns,
                "total_invoices": self.total_invoices,
                "total_line_items": self.total_line_items,
                "processing_time_seconds": self.processing_time_seconds,
                "success_rate_percentage": self.success_rate_percentage,
                "total_declared_value_usd": float(self.get_total_declared_value()),
                "processing_stats": self.get_processing_stats(),
                "confidence_summary": self.get_confidence_summary()
            },
            "esn_data": {
                esn: {
                    "esn_metadata": {
                        "total_invoices": esn_data.total_invoices,
                        "total_line_items": esn_data.total_line_items,
                        "total_declared_value_usd": float(esn_data.total_declared_value_usd),
                        "processing_status": esn_data.processing_status,
                        "success_rate_percentage": esn_data.get_success_rate(),
                        "unique_suppliers": esn_data.get_unique_suppliers(),
                        "date_range": esn_data.get_date_range(),
                        "confidence_distribution": esn_data.get_confidence_distribution()
                    },
                    "invoices": {
                        invoice_file: {
                            "invoice_metadata": {
                                "supplier": invoice.supplier,
                                "invoice_date": invoice.invoice_date,
                                "invoice_number": invoice.invoice_number,
                                "total_line_items": invoice.total_line_items,
                                "invoice_total_usd": float(invoice.invoice_total_usd),
                                "calculated_total_usd": float(invoice.get_total_calculated()),
                                "totals_match": invoice.validate_totals(),
                                "extraction_confidence": invoice.extraction_confidence.value,
                                "processing_time_seconds": invoice.processing_time_seconds,
                                "extraction_notes": invoice.extraction_notes,
                                "unique_skus": invoice.get_unique_skus(),
                                "skus_by_tariff": invoice.get_skus_by_tariff()
                            },
                            "line_items": [
                                {
                                    "line_number": item.line_number,
                                    "sku_client_reference": item.sku_client_reference,
                                    "material_description": item.material_description,
                                    "customs_quantity": item.customs_quantity,
                                    "customs_unit": item.customs_unit,
                                    "tariff_fraction": item.tariff_fraction,
                                    "unit_value_usd": item.unit_value_usd,
                                    "total_value_usd": float(item.total_value_usd)
                                }
                                for item in invoice.line_items
                            ]
                        }
                        for invoice_file, invoice in esn_data.invoices.items()
                    }
                }
                for esn, esn_data in self.esn_data.items()
            }
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)