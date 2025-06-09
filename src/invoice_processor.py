import asyncio
import logging
from typing import List
from decimal import Decimal
from pathlib import Path

from llama_parse import LlamaParse
from llama_index.llms.openai import OpenAI
from llama_index.core.prompts import ChatPromptTemplate

from models import CommercialInvoiceData, ConfidenceLevel
from config import SystemConfig

logger = logging.getLogger(__name__)

class InvoiceProcessor:
    """Processes invoice PDFs using LlamaParse + OpenAI hybrid approach - PRODUCTION VERSION"""
    
    def __init__(self, config: SystemConfig):
        self.config = config
        
        # Initialize LlamaParse for PDF processing
        self.parser = LlamaParse(
            api_key=config.LLAMA_CLOUD_API_KEY,
            result_type="markdown",
            premium_mode=True,  # Better for complex Mexican invoices
            language="es",  # Spanish support
            parsing_instruction="""
            This is a Mexican commercial invoice (Factura Comercial). Please extract all text carefully, 
            paying special attention to:
            - Final total amounts (Total, Importe Total, Total a Pagar, Gran Total)
            - Currency information (USD, MXN, Pesos, Dolares)
            - Exchange rates if mentioned
            - Line items and subtotals
            - Company information and invoice numbers
            - Tax information (IVA, taxes)
            
            Preserve the exact formatting of monetary amounts.
            """
        )
        
        # Initialize OpenAI for structured extraction
        self.llm = OpenAI(
            api_key=config.OPENAI_API_KEY,
            model="gpt-4o",  # Use latest model for best accuracy
            temperature=0.1  # Low temperature for consistent extraction
        )
        
        # Optimized extraction prompt for Mexican commercial invoices
        self.extraction_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert at extracting data from Mexican commercial invoices (Facturas Comerciales).

Your PRIMARY GOAL is to find the FINAL TOTAL USD AMOUNT with maximum accuracy.

CRITICAL GUIDELINES FOR MEXICAN INVOICES:
1. Look for final totals with these patterns:
   - "Total", "Importe Total", "Total a Pagar", "Gran Total"
   - "Total USD", "Total Dolares", "Total en Dolares"
   - Final amounts after all taxes and discounts

2. Currency handling:
   - If amount is clearly marked as USD/Dolares: use directly
   - If amount is in MXN/Pesos: convert using exchange rate mentioned in document
   - If no exchange rate given: note original currency and amount

3. Mexican number formats:
   - Use period (.) for decimals: 1,234.56
   - Use comma (,) for thousands: $1,234.56
   - Watch for: $1,234.56 USD or $1,234.56 MXN

4. Set confidence levels:
   - HIGH: Clear total amount in USD, clearly marked and unambiguous
   - MEDIUM: Total calculated from subtotals, or currency conversion needed
   - LOW: Ambiguous amounts, unclear currency, or estimated
   - ERROR: Cannot determine any reasonable amount

5. Mexican invoice features to recognize:
   - CFDI (Comprobante Fiscal Digital)
   - RFC (tax ID)
   - IVA (Mexican VAT)
   - QR codes
   - Digital stamps

EXTRACT the exact text containing the total amount for audit purposes."""),

            ("user", """Extract invoice data focusing on the FINAL TOTAL USD AMOUNT.

Be extremely careful with:
- Currency identification (USD vs MXN vs Pesos)
- Number formatting (decimals and thousands separators)
- Final vs subtotal amounts
- Tax inclusions

If the invoice is in MXN/Pesos and includes an exchange rate, convert to USD.
If no exchange rate is provided but amount is in MXN, note this in extraction_notes.

Invoice content:
{invoice_content}
""")
        ])
    
    async def process_single_invoice(self, pdf_path: str, esn: str) -> CommercialInvoiceData:
        """Process a single invoice PDF file - PRODUCTION VERSION"""
        try:
            invoice_filename = Path(pdf_path).name
            logger.info(f"📄 Processing invoice: {invoice_filename}")
            
            # Step 1: Parse PDF with LlamaParse
            docs = await self.parser.aload_data(pdf_path)
            
            if not docs:
                raise ValueError("No content extracted from PDF by LlamaParse")
            
            # Combine all pages with clear page markers
            invoice_content = "\n\n".join([
                f"=== PAGE {i+1} ===\n{doc.get_content()}" 
                for i, doc in enumerate(docs)
            ])
            
            if len(invoice_content.strip()) < 50:
                raise ValueError("PDF content too short - possible parsing error")
            
            logger.debug(f"Extracted {len(invoice_content)} characters from PDF")
            
            # Step 2: Extract structured data with OpenAI
            extracted_data = await self.llm.astructured_predict(
                CommercialInvoiceData,
                self.extraction_prompt,
                invoice_content=invoice_content
            )
            
            # Step 3: Post-process and validate
            if extracted_data.total_usd_amount <= 0:
                extracted_data.confidence_level = ConfidenceLevel.ERROR
                extracted_data.extraction_notes = "No valid positive amount extracted"
                logger.warning(f"⚠️  No valid amount extracted from {invoice_filename}")
            elif extracted_data.total_usd_amount > 1000000:  # Sanity check for very large amounts
                logger.warning(f"⚠️  Very large amount extracted: ${extracted_data.total_usd_amount}")
                extracted_data.confidence_level = ConfidenceLevel.LOW
                extracted_data.extraction_notes = "Amount seems unusually large - please verify"
            
            # Ensure basic data is present
            if not extracted_data.invoice_number:
                extracted_data.invoice_number = f"EXTRACTED_{Path(pdf_path).stem}"
            
            if not extracted_data.company_name:
                extracted_data.company_name = "UNKNOWN_COMPANY"
            
            # Log successful extraction
            confidence_icon = "🟢" if extracted_data.confidence_level == ConfidenceLevel.HIGH else "🟡" if extracted_data.confidence_level == ConfidenceLevel.MEDIUM else "🔴"
            logger.info(f"{confidence_icon} Extracted: {extracted_data.invoice_number} = ${extracted_data.total_usd_amount} ({extracted_data.confidence_level.value})")
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"❌ Error processing {invoice_filename}: {str(e)}")
            
            # Return comprehensive error result
            return CommercialInvoiceData(
                invoice_number=f"ERROR_{Path(pdf_path).stem}",
                company_name="PROCESSING_ERROR",
                total_usd_amount=Decimal('0'),
                confidence_level=ConfidenceLevel.ERROR,
                extraction_notes=f"Processing failed: {str(e)[:200]}...",  # Truncate long error messages
                currency="UNKNOWN"
            )
    
    async def process_esn_invoices(self, esn: str, pdf_files: List[str]) -> List[CommercialInvoiceData]:
        """Process all invoices for an ESN with controlled concurrency - PRODUCTION VERSION"""
        
        if not pdf_files:
            logger.warning(f"No PDF files to process for ESN {esn}")
            return []
        
        logger.info(f"⚡ Processing {len(pdf_files)} invoices for ESN {esn}")
        
        # Control concurrency to avoid API rate limits
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENT_PDFS)
        
        async def process_with_semaphore(pdf_path: str):
            async with semaphore:
                # Add small delay to avoid hitting rate limits
                await asyncio.sleep(0.5)
                return await self.process_single_invoice(pdf_path, esn)
        
        # Process all invoices
        tasks = [process_with_semaphore(pdf_path) for pdf_path in pdf_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle results and exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception processing {Path(pdf_files[i]).name}: {result}")
                # Create error result for exception
                error_result = CommercialInvoiceData(
                    invoice_number=f"EXCEPTION_{Path(pdf_files[i]).stem}",
                    company_name="EXCEPTION_ERROR",
                    total_usd_amount=Decimal('0'),
                    confidence_level=ConfidenceLevel.ERROR,
                    extraction_notes=f"Exception during processing: {str(result)[:200]}"
                )
                valid_results.append(error_result)
            else:
                valid_results.append(result)
        
        # Log processing summary
        successful = len([r for r in valid_results if r.confidence_level != ConfidenceLevel.ERROR])
        total_amount = sum(r.total_usd_amount for r in valid_results if r.confidence_level != ConfidenceLevel.ERROR)
        
        logger.info(f"📊 ESN {esn} Summary: {successful}/{len(valid_results)} successful, Total: ${total_amount}")
        
        return valid_results