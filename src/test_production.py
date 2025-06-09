import asyncio
import logging
from config import SystemConfig
from main import ComplianceSystemOrchestrator

async def test_production_system():
    """Test the production system with real data"""
    
    print("🧪 TESTING PRODUCTION SYSTEM")
    print("=" * 50)
    
    try:
        # Test configuration
        config = SystemConfig()
        if not config.validate():
            print("❌ Configuration failed")
            return False
        
        print("✅ Configuration validated")
        
        # Initialize system
        system = ComplianceSystemOrchestrator(config)
        print("✅ System initialized")
        
        # Test Google services
        esn_folders = system.google_manager.get_all_esn_folders()
        print(f"✅ Found {len(esn_folders)} ESN folders")
        
        if esn_folders:
            # Test single ESN
            test_esn = esn_folders[0]
            print(f"🎯 Testing ESN: {test_esn['esn']}")
            
            result = await system.process_single_esn(test_esn['esn'], test_esn['folder_id'])
            
            print(f"✅ Test result: {result.status.value}")
            print(f"   Declared: ${result.declared_amount}")
            print(f"   Calculated: ${result.calculated_amount}")
            print(f"   Invoices: {result.invoice_count}")
        
        print("\n🎉 Production test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_production_system())