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
    total_usd_amount: Decimal = Field(description="FINAL TOTAL USD AMOUNT", gt=0)
    currency: str = Field(default="USD", description="Original currency")
    
    # Quality control
    amount_source_text: Optional[str] = Field(None, description="Exact text with total amount")
    confidence_level: ConfidenceLevel = Field(default=ConfidenceLevel.MEDIUM, description="Extraction confidence")
    extraction_notes: Optional[str] = Field(None, description="Processing notes")

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