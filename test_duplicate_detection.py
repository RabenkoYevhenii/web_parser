#!/usr/bin/env python3
"""
Test script to verify duplicate detection by source_url
"""

import json
from tools import UniversalCompanyScraper

def test_duplicate_detection():
    """Test the source_url based duplicate detection"""
    
    print("Testing duplicate detection by source_url...")
    
    # Create a test scraper instance
    scraper = UniversalCompanyScraper("https://example.com")
    
    # Test data - companies with different URLs
    test_companies = [
        {
            "name": "Test Company 1",
            "source_url": "https://example.com/company1",
            "website_url": "https://company1.com",
            "description": "First test company"
        },
        {
            "name": "Test Company 2", 
            "source_url": "https://example.com/company2",
            "website_url": "https://company2.com",
            "description": "Second test company"
        },
        {
            "name": "Test Company 1 Duplicate",  # Different name but same source_url
            "source_url": "https://example.com/company1",  # Same URL as first company
            "website_url": "https://different-site.com",
            "description": "This should be detected as duplicate"
        },
        {
            "name": "Test Company 1 URL Variant",  # URL with query parameters
            "source_url": "https://example.com/company1?param=123#section",  # Same base URL
            "website_url": "https://another-site.com", 
            "description": "This should also be detected as duplicate"
        },
        {
            "name": "Test Company 1 Trailing Slash",  # URL with trailing slash
            "source_url": "https://example.com/company1/",  # Same base URL with trailing slash
            "website_url": "https://yet-another-site.com",
            "description": "This should also be detected as duplicate"
        }
    ]
    
    print("\n--- Testing company save and duplicate detection ---")
    
    saved_count = 0
    skipped_count = 0
    
    for i, company in enumerate(test_companies, 1):
        print(f"\nTesting company {i}: {company['name']}")
        print(f"Source URL: {company['source_url']}")
        
        # Check if URL is already processed before saving
        if scraper.is_source_url_processed(company['source_url']):
            print(f"❌ URL already processed (pre-check): {company['source_url']}")
            skipped_count += 1
            continue
            
        # Try to save the company
        was_saved = scraper._save_company_immediately(company)
        
        if was_saved:
            print(f"✅ Company saved successfully")
            saved_count += 1
        else:
            print(f"❌ Company skipped as duplicate") 
            skipped_count += 1
    
    print(f"\n--- Results ---")
    print(f"Companies saved: {saved_count}")
    print(f"Companies skipped as duplicates: {skipped_count}")
    print(f"Total companies in scraper: {len(scraper.companies_data)}")
    
    # Show the saved companies
    print(f"\n--- Saved Companies ---")
    for i, company in enumerate(scraper.companies_data, 1):
        print(f"{i}. {company.get('name', 'Unknown')} - {company.get('source_url', 'No URL')}")
    
    # Test the pre-check method
    print(f"\n--- Testing pre-check method ---")
    test_urls = [
        "https://example.com/company1",
        "https://example.com/company1?different=param",
        "https://example.com/company1/",
        "https://example.com/company2",
        "https://example.com/company3"  # This one shouldn't exist
    ]
    
    for url in test_urls:
        is_processed = scraper.is_source_url_processed(url)
        status = "✅ Already processed" if is_processed else "❌ Not processed"
        print(f"{status}: {url}")
    
    # Clean up test files
    try:
        import os
        os.remove(scraper.csv_filename)
        os.remove(scraper.json_filename)
        print(f"\n--- Cleanup ---")
        print("Test files cleaned up successfully")
    except:
        print("Note: Could not clean up test files")

if __name__ == "__main__":
    test_duplicate_detection()
