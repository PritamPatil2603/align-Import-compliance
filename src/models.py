from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import datetime, date
from pydantic import BaseModel, Field
from enum import Enum

class ProcessingStatus(str, Enum):
    """Processing status enumeration"""
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    ERROR = "ERROR"
    PENDING = "PENDING"

class ConfidenceLevel(str, Enum):
    """Extraction confidence levels"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    ERROR = "ERROR"

class CommercialInvoiceData(BaseModel):
    """Complete commercial invoice extraction result"""
    # Basic invoice info
    invoice_number: str = Field(description="Invoice number/reference")
    invoice_date: Optional[date] = Field(None, description="Invoice date")
    company_name: str = Field(description="Issuing company name")
    
    # Financial data (main focus)
    total_usd_amount: Decimal = Field(description="FINAL TOTAL USD AMOUNT", ge=0)
    currency: str = Field(default="USD", description="Original currency")
    
    # Quality control
    amount_source_text: Optional[str] = Field(None, description="Exact text with total amount")
    confidence_level: ConfidenceLevel = Field(default=ConfidenceLevel.MEDIUM, description="Extraction confidence")
    extraction_notes: Optional[str] = Field(None, description="Processing notes")
    
    # Existing enhanced fields
    client_reference: Optional[str] = Field(None, description="SKU/Product ID/Reference number")
    material_description: Optional[str] = Field(None, description="Product/material description")
    
    # ADD THESE NEW FIELDS:
    fecha_hora: Optional[str] = Field(None, description="Fecha y hora de emisión (Date and time)")
    cantidad_total: Optional[float] = Field(None, description="CANTIDAD TOTAL (Total units/quantity)")
    valor_unitario: Optional[float] = Field(None, description="VALOR UNITARIO (Unit value/price)")

class ESNProcessingResult(BaseModel):
    """Result of processing all invoices for one ESN"""
    # Basic info
    esn: str = Field(description="Entry Summary Number")
    status: ProcessingStatus = Field(description="Processing status")
    
    # Amounts
    declared_amount: Decimal = Field(description="Amount from Google Sheets")
    calculated_amount: Decimal = Field(description="Sum of invoice amounts")
    difference: Decimal = Field(description="Absolute difference")
    percentage_difference: float = Field(description="Percentage difference")
    
    # Processing details
    invoice_count: int = Field(description="Number of invoices processed")
    successful_extractions: int = Field(default=0, description="Successful extractions")
    failed_extractions: int = Field(default=0, description="Failed extractions")
    
    # Invoice details
    processed_invoices: List[Dict[str, Any]] = Field(default_factory=list)
    processing_errors: List[str] = Field(default_factory=list)
    
    # Metadata
    processed_at: datetime = Field(default_factory=datetime.now)
    processing_time_seconds: Optional[float] = Field(None)

class ComplianceReport(BaseModel):
    """Complete compliance verification report"""
    # Report metadata
    report_id: str = Field(description="Unique report identifier")
    generated_at: datetime = Field(default_factory=datetime.now)
    
    # Summary statistics
    total_esns_processed: int = Field(description="Total ESNs processed")
    successful_matches: int = Field(description="ESNs within tolerance")
    discrepancies_found: int = Field(description="ESNs with mismatches")
    processing_errors: int = Field(description="ESNs with errors")
    
    # Financial summary
    total_declared_amount: Decimal = Field(description="Sum of declared amounts")
    total_calculated_amount: Decimal = Field(description="Sum of calculated amounts")
    compliance_rate: float = Field(description="Percentage compliance rate")
    
    # Detailed results
    esn_results: List[ESNProcessingResult] = Field(description="Per-ESN results")

# Add these NEW classes to src/models.py (after the existing CommercialInvoiceData class):

from typing import List
from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import date
from enum import Enum

# Keep your existing ConfidenceLevel and CommercialInvoiceData classes as they are

class LineItem(BaseModel):
    """Individual line item for detailed invoice breakdown"""
    line_number: int = Field(description="Sequential line number")
    sku: str = Field(description="Product SKU/Reference number")
    description: str = Field(description="Product description")
    quantity: float = Field(description="Quantity for this line item", ge=0)
    unit_price: Decimal = Field(description="Price per unit in USD", ge=0)
    line_total: Decimal = Field(description="Total for this line (qty * unit_price)", ge=0)
    
    # Optional compliance fields
    unit_of_measure: Optional[str] = Field(None, description="Unit (ML, KG, PCS, etc.)")
    country_of_origin: Optional[str] = Field(None, description="Country of origin")
    hts_code: Optional[str] = Field(None, description="HTS tariff classification code")

class EnhancedInvoiceData(BaseModel):
    """Enhanced invoice data with separated line items"""
    
    # Invoice header (preserve existing structure)
    invoice_number: str = Field(description="Invoice number/reference")
    invoice_date: Optional[date] = Field(None, description="Invoice date")
    company_name: str = Field(description="Issuing company name")
    fecha_hora: Optional[str] = Field(None, description="Fecha y hora de emisión")
    
    # Financial totals
    total_usd_amount: Decimal = Field(description="Total invoice amount USD", ge=0)
    currency: str = Field(default="USD", description="Invoice currency")
    
    # Line items (NEW)
    line_items: List[LineItem] = Field(default=[], description="Individual line items")
    total_line_items: int = Field(default=0, description="Number of line items")
    line_items_total: Decimal = Field(default=Decimal('0'), description="Sum of all line totals")
    
    # Quality control (preserve existing)
    confidence_level: ConfidenceLevel = Field(default=ConfidenceLevel.MEDIUM, description="Extraction confidence")
    extraction_notes: Optional[str] = Field(None, description="Processing notes")
    amount_source_text: Optional[str] = Field(None, description="Source text for amount")
    
    # Legacy compatibility fields (for backward compatibility)
    client_reference: Optional[str] = Field(None, description="Combined SKUs (legacy)")
    material_description: Optional[str] = Field(None, description="Combined descriptions (legacy)")
    cantidad_total: Optional[float] = Field(None, description="Total quantity (legacy)")
    valor_unitario: Optional[float] = Field(None, description="Average unit value (legacy)")

class InvoiceExtractionResult(BaseModel):
    """Complete extraction result with both formats"""
    
    # Primary enhanced format
    enhanced_data: EnhancedInvoiceData = Field(description="Enhanced line item format")
    
    # Legacy format (for compatibility)
    legacy_data: CommercialInvoiceData = Field(description="Original format for compatibility")
    
    # Processing metadata
    processing_time: float = Field(description="Processing time in seconds")
    extraction_method: str = Field(description="Method used for extraction")
    line_item_extraction_success: bool = Field(description="Whether line items were successfully separated")