# Quick MongoDB connection test

import os
from dotenv import load_dotenv
from pymongo import MongoClient
import sys

def test_mongodb_connection():
    """Test MongoDB Atlas connection"""
    
    load_dotenv()
    
    mongodb_uri = os.getenv('MONGODB_URI')
    mongodb_database = os.getenv('MONGODB_DATABASE')
    
    print("🔍 Testing MongoDB Atlas Connection")
    print("=" * 40)
    
    if not mongodb_uri:
        print("❌ MONGODB_URI not found in .env file")
        return False
    
    if not mongodb_database:
        print("❌ MONGODB_DATABASE not found in .env file")
        return False
    
    print(f"📊 Database: {mongodb_database}")
    print(f"🔗 URI: {mongodb_uri[:50]}...")
    
    try:
        # Test connection
        client = MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=20000
        )
        
        # Ping the database
        client.admin.command('ping')
        print("✅ MongoDB connection successful!")
        
        # Get database
        db = client[mongodb_database]
        
        # List collections
        collections = db.list_collection_names()
        print(f"📋 Available collections: {collections}")
        
        # Test sku_data collection
        if 'sku_data' in collections:
            org_id = "dff4dbb5-e2cb-49b3-8ae4-082418ac1db2"
            count = db.sku_data.count_documents({"organization_id": org_id})
            print(f"🏷️ SKUs for Maesa organization: {count:,}")
            
            # Sample record
            sample = db.sku_data.find_one({"organization_id": org_id})
            if sample:
                print(f"📄 Sample SKU fields: {list(sample.keys())}")
        
        # Test sku_duties collection
        if 'sku_duties' in collections:
            duties_count = db.sku_duties.count_documents({})
            print(f"🤝 Total SKU duties records: {duties_count:,}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_mongodb_connection()
    if success:
        print("\n🎉 MongoDB setup is ready!")
        print("Next step: Run SKU validation")
    else:
        print("\n⚠️ Fix MongoDB connection before proceeding")
        sys.exit(1)