# ============================================
# FILE: src/invoice_processor.py
# PRODUCTION-READY optimized invoice processor
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

from models import CommercialInvoiceData, ConfidenceLevel
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
    """Production-ready optimized invoice processor with advanced features"""
    
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
            
            # SPEED OPTIMIZATIONS - ADD THESE LINES:
            premium_mode=False,           # 30-40% faster than premium
            language="es",                # Spanish optimization
            max_timeout=60,               # Faster timeout
            show_progress=False,          # Reduce overhead
            
            # Keep existing settings:
            parsing_instruction=config.PARSING_INSTRUCTION if hasattr(config, 'PARSING_INSTRUCTION') else "Extract commercial invoice data",
            verbose=False
        )
        
        # OPTIMIZED OpenAI settings for speed
        self.llm = OpenAI(
            api_key=config.OPENAI_API_KEY,
            model="gpt-4o-mini",          # Fastest model
            temperature=0.1,
            
            # SPEED OPTIMIZATIONS - ADD THESE LINES:
            max_tokens=600,               # Limit response length
            timeout=30.0,                 # Faster timeout
        )
        
        # Optimized extraction prompt
        self.extraction_prompt = ChatPromptTemplate.from_messages([
            ("system", """Extract the FINAL TOTAL USD AMOUNT from this commercial invoice.

FOCUS ON:
- Final totals: "Total", "Importe Total", "Gran Total"
- USD amounts (convert from MXN if exchange rate given)
- Amount after all taxes and fees

CONFIDENCE:
- HIGH: Clear USD total, unambiguous
- MEDIUM: Requires calculation or conversion
- LOW: Estimated or unclear
- ERROR: Cannot determine amount"""),
            
            ("user", "Extract the final total USD amount:\n\n{invoice_content}")
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
    
    async def process_esn_invoices(self, esn: str, pdf_files: List[str]) -> List[CommercialInvoiceData]:
        """Optimized ESN processing with intelligent concurrency"""
        
        if not pdf_files:
            logger.warning(f"No PDF files to process for ESN {esn}")
            return []
        
        logger.info(f"⚡ Processing {len(pdf_files)} invoices for ESN {esn}")
        
        # Dynamic concurrency based on file count and system resources
        optimal_concurrency = min(
            self.config.MAX_CONCURRENT_PDFS * 2,  # Base concurrency
            len(pdf_files),  # Don't exceed file count
            8,  # Maximum concurrent processes
            max(2, os.cpu_count() or 2)  # Consider system resources
        )
        
        semaphore = asyncio.Semaphore(optimal_concurrency)
        
        async def process_with_semaphore(pdf_path: str):
            async with semaphore:
                # Stagger requests to avoid API rate limits
                await asyncio.sleep(0.1)
                return await self.process_single_invoice(pdf_path, esn)
        
        # Process all invoices concurrently
        start_time = time.time()
        tasks = [process_with_semaphore(pdf_path) for pdf_path in pdf_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle results and exceptions
        valid_results = []
        exception_count = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                exception_count += 1
                logger.error(f"Exception processing {Path(pdf_files[i]).name}: {result}")
                
                # Create error result for exception
                error_result = CommercialInvoiceData(
                    invoice_number=f"EXCEPTION_{Path(pdf_files[i]).stem}",
                    company_name="EXCEPTION_ERROR",
                    total_usd_amount=Decimal('0'),
                    confidence_level=ConfidenceLevel.ERROR,
                    extraction_notes=f"Exception: {str(result)[:200]}"
                )
                valid_results.append(error_result)
            else:
                valid_results.append(result)
        
        # Log comprehensive summary
        total_time = time.time() - start_time
        successful = len([r for r in valid_results if r.confidence_level != ConfidenceLevel.ERROR])
        total_amount = sum(r.total_usd_amount for r in valid_results if r.confidence_level != ConfidenceLevel.ERROR)
        avg_time = total_time / len(pdf_files) if pdf_files else 0
        
        logger.info(f"📊 ESN {esn} Summary:")
        logger.info(f"   ✅ Successful: {successful}/{len(valid_results)}")
        logger.info(f"   💰 Total: ${total_amount:,.2f}")
        logger.info(f"   ⚠️  Exceptions: {exception_count}")
        logger.info(f"   ⏱️  Time: {total_time:.1f}s ({avg_time:.1f}s avg/PDF)")
        logger.info(f"   🔄 Concurrency: {optimal_concurrency}")
        
        return valid_results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        stats = self.processing_stats.copy()
        
        if stats['total_processed'] > 0:
            stats['cache_hit_rate'] = stats['cache_hits'] / stats['total_processed'] * 100
            stats['success_rate'] = stats['successful_extractions'] / stats['total_processed'] * 100
            stats['avg_processing_time'] = stats['total_processing_time'] / stats['total_processed']
        
        return stats
    
    def reset_stats(self):
        """Reset performance statistics"""
        self.processing_stats = {
            'total_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_processing_time': 0
        }

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
    
    async def benchmark_concurrent_processing(self, pdf_files: List[str], concurrency_levels: List[int]) -> Dict[int, float]:
        """Benchmark different concurrency levels"""
        results = {}
        
        for concurrency in concurrency_levels:
            # Temporarily set concurrency
            original_max = self.processor.config.MAX_CONCURRENT_PDFS
            self.processor.config.MAX_CONCURRENT_PDFS = concurrency
            
            start_time = time.time()
            await self.processor.process_esn_invoices("BENCHMARK", pdf_files[:concurrency*2])
            total_time = time.time() - start_time
            
            results[concurrency] = total_time
            
            # Restore original setting
            self.processor.config.MAX_CONCURRENT_PDFS = original_max
        
        return results

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