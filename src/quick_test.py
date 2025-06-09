import asyncio
from test_single_esn import SingleESNTester

async def quick_test_esn(esn_name: str):
    """Quick test for a specific ESN (for advanced users)"""
    
    print(f"🚀 QUICK TEST FOR ESN: {esn_name}")
    print("=" * 40)
    
    tester = SingleESNTester()
    result = await tester.test_specific_esn(esn_name)
    
    if result:
        print(f"\n✅ Test completed for {esn_name}")
        print(f"Declared: ${result['declared_amount']:,.2f}")
        print(f"Calculated: ${result['calculated_amount']:,.2f}")
        print(f"Difference: {result['percentage_difference']:.2f}%")
    else:
        print(f"\n❌ Test failed for {esn_name}")

# Usage examples:
# python quick_test.py  # Will prompt for ESN name
# python -c "import asyncio; from quick_test import quick_test_esn; asyncio.run(quick_test_esn('AE900683929'))"

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        esn = sys.argv[1]
        asyncio.run(quick_test_esn(esn))
    else:
        esn = input("Enter ESN to test: ").strip()
        if esn:
            asyncio.run(quick_test_esn(esn))
        else:
            print("No ESN provided")