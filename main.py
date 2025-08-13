import asyncio
from asyncio.log import logger
from tools import UniversalCompanyScraper


async def main(url: str = '', headless: bool = False, timeout: int = 30000):
    """
    Main function to run the universal scraper
    
    Args:
        url: URL of the website to scrape (required)
        headless: Whether to run browser in headless mode
        timeout: Timeout for page operations in milliseconds
    """
    if not url:
        print("Error: URL is required!")
        print("Usage: await main('https://example.com/companies')")
        return
    
    print(f"\n{'='*60}")
    print(f"Starting Universal Company Scraper")
    print(f"Target URL: {url}")
    print(f"Mode: {'Headless' if headless else 'Visible'}")
    print(f"{'='*60}\n")
    
    # Initialize scraper
    scraper = UniversalCompanyScraper(
        base_url=url,
        headless=headless,
        timeout=timeout
    )
    
    try:
        # Scrape companies
        companies_data = await scraper.scrape_companies()
        
        # Save data
        scraper.save_to_csv()
        
        # Print summary
        scraper.print_summary()
        
        return companies_data
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        return []


if __name__ == "__main__":
    # Example usage with different websites
    import sys
    
    # Get URL from command line argument if provided
    if len(sys.argv) > 1:
        target_url = sys.argv[1]
    else:
        # Default example URL
        target_url = "https://fieraroma-2025.expofp.com/"
        print(f"No URL provided, using: {target_url}")
    
    # Run the scraper
    asyncio.run(main(
        url=target_url,
        headless=False,  # Set to True for production
        timeout=30000
    ))