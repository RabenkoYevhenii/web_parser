import asyncio
import json
import re
import csv
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
from playwright.async_api import (
    async_playwright,
    Page,
    Browser,
    BrowserContext,
)
import logging
import time
from pathlib import Path

from helpers import helpers

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UniversalCompanyScraper:
    def __init__(
        self, base_url: str, headless: bool = False, timeout: int = 30000
    ):
        """
        Initialize the universal scraper

        Args:
            base_url: Base URL of the website to scrape
            headless: Whether to run browser in headless mode
            timeout: Default timeout for page operations in milliseconds
        """
        self.base_url = base_url
        self.parsed_url = urlparse(base_url)
        self.domain = self.parsed_url.netloc.replace("www.", "").replace(
            ".", "_"
        )
        self.headless = headless
        self.timeout = timeout
        self.companies_data = []

        # File paths for saving data
        self.csv_filename = f"{self.domain}_companies.csv"
        self.json_filename = f"{self.domain}_companies.json"

        # Set to track processed companies (to avoid duplicates)
        self.processed_companies = set()

        # Load existing data if files exist
        self._load_existing_data()

        # Comprehensive list of social media domains
        self.social_domains = helpers.social_domains

    def _load_existing_data(self):
        """
        Load existing data from files to avoid duplicates
        """
        try:
            # Load from JSON file if it exists
            if Path(self.json_filename).exists():
                with open(self.json_filename, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                    self.companies_data = existing_data

                    # Build set of processed companies for duplicate checking
                    for company in existing_data:
                        company_key = self._generate_company_key(company)
                        self.processed_companies.add(company_key)

                    logger.info(
                        f"Loaded {len(existing_data)} existing companies from {self.json_filename}"
                    )
            else:
                logger.info("No existing data file found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading existing data: {str(e)}")
            self.companies_data = []
            self.processed_companies = set()

    def _generate_company_key(self, company_data: Dict) -> str:
        """
        Generate a unique key for a company to check for duplicates

        Args:
            company_data: Company data dictionary

        Returns:
            str: Unique key for the company
        """
        try:
            # Use combination of name, website, and source_url to identify duplicates
            # Handle None values properly
            name = company_data.get("name") or ""
            website = company_data.get("website_url") or ""
            source_url = company_data.get("source_url") or ""

            # Convert to lowercase for comparison
            name = name.strip().lower() if name else ""
            website = website.strip().lower() if website else ""
            source_url = source_url.strip().lower() if source_url else ""

            # Create a composite key
            key_parts = []
            if name:
                key_parts.append(f"name:{name}")
            if website:
                key_parts.append(f"website:{website}")
            if source_url:
                key_parts.append(f"source:{source_url}")

            # If no meaningful data, use source_url as fallback
            if not key_parts and source_url:
                key_parts.append(f"source:{source_url}")

            return "|".join(key_parts)

        except Exception as e:
            logger.error(
                f"Error generating company key: {str(e)}, company_data: {company_data}"
            )
            # Return a fallback key
            return f"fallback:{id(company_data)}"

    def _is_duplicate_company(self, company_data: Dict) -> bool:
        """
        Check if a company is already processed

        Args:
            company_data: Company data dictionary

        Returns:
            bool: True if company is a duplicate
        """
        company_key = self._generate_company_key(company_data)
        return company_key in self.processed_companies

    def _categorize_social_media(
        self, socials_list: Optional[List[str]]
    ) -> Dict[str, str]:
        """
        Categorize social media links into specific platforms

        Args:
            socials_list: List of social media URLs (can be None)

        Returns:
            Dict: Dictionary with categorized social media links
        """
        categorized = {
            "facebook": "",
            "instagram": "",
            "linkedin": "",
            "twitter": "",
            "other_socials": [],
        }

        if not socials_list:
            return {
                **categorized,
                "other_socials": "",
            }  # Convert list to string

        for social_url in socials_list:
            if not social_url:
                continue

            url_lower = social_url.lower()

            # Categorize by platform
            if any(
                platform in url_lower
                for platform in ["facebook.com", "fb.com", "fb.me"]
            ):
                if not categorized["facebook"]:  # Take first one found
                    categorized["facebook"] = social_url
            elif any(
                platform in url_lower
                for platform in ["instagram.com", "instagr.am"]
            ):
                if not categorized["instagram"]:
                    categorized["instagram"] = social_url
            elif any(
                platform in url_lower
                for platform in ["linkedin.com", "lnkd.in"]
            ):
                if not categorized["linkedin"]:
                    categorized["linkedin"] = social_url
            elif any(
                platform in url_lower
                for platform in ["twitter.com", "t.co", "x.com"]
            ):
                if not categorized["twitter"]:
                    categorized["twitter"] = social_url
            else:
                # All other social platforms
                categorized["other_socials"].append(social_url)

        # Convert other_socials list to string
        categorized["other_socials"] = " | ".join(categorized["other_socials"])

        return categorized

    def _save_company_immediately(self, company_data: Dict) -> bool:
        """
        Save a single company immediately to both CSV and JSON files

        Args:
            company_data: Company data dictionary

        Returns:
            bool: True if company was saved (not a duplicate)
        """
        try:
            # Validate company_data
            if not isinstance(company_data, dict):
                logger.error(
                    f"Invalid company_data type: {type(company_data)}"
                )
                return False

            # Check for duplicates
            if self._is_duplicate_company(company_data):
                logger.info(
                    f"Skipping duplicate company: {company_data.get('name', 'Unknown')}"
                )
                return False

            # Update company index to be sequential
            company_data["company_index"] = len(self.companies_data) + 1

            # Add to processed set
            company_key = self._generate_company_key(company_data)
            self.processed_companies.add(company_key)

            # Add to companies data
            self.companies_data.append(company_data)

            # Save to JSON immediately
            with open(self.json_filename, "w", encoding="utf-8") as f:
                json.dump(self.companies_data, f, indent=2, ensure_ascii=False)

            # Save to CSV immediately with separate social media columns
            fieldnames = [
                "company_index",
                "name",
                "description",
                "website_url",
                "phone",
                "email",
                "logo_url",
                "source_url",
                "facebook",
                "instagram",
                "linkedin",
                "twitter",
                "other_socials",
            ]

            # Check if CSV file exists and has header
            csv_exists = Path(self.csv_filename).exists()

            with open(
                self.csv_filename,
                "a" if csv_exists else "w",
                newline="",
                encoding="utf-8",
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header if file is new
                if not csv_exists:
                    writer.writeheader()

                # Prepare company data for CSV
                company_copy = company_data.copy()

                # Categorize social media links
                socials = company_data.get("socials", [])
                social_categories = self._categorize_social_media(socials)

                # Add categorized social media to company data
                company_copy.update(social_categories)

                # Remove the original socials field for CSV (keep in JSON)
                if "socials" in company_copy:
                    del company_copy["socials"]

                # Ensure all fields exist and handle None values
                for field in fieldnames:
                    if (
                        field not in company_copy
                        or company_copy[field] is None
                    ):
                        company_copy[field] = ""

                writer.writerow(company_copy)

            logger.info(
                f"Saved company: {company_data.get('name', 'Unknown')} (Total: {len(self.companies_data)})"
            )
            return True

        except Exception as e:
            logger.error(f"Error saving company immediately: {str(e)}")
            logger.error(f"Company data that caused error: {company_data}")
            # Print stack trace for debugging
            import traceback

            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

    def normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize URL to absolute format

        Args:
            url: URL to normalize

        Returns:
            str: Normalized absolute URL
        """
        if not url:
            return None

        # Handle protocol-relative URLs
        if url.startswith("//"):
            return "https:" + url

        # Handle relative URLs
        elif url.startswith("/"):
            return urljoin(self.base_url, url)

        # Handle URLs without protocol
        elif not url.startswith(("http://", "https://")):
            # Check if it looks like a domain
            if "." in url and not url.startswith("."):
                return "https://" + url
            else:
                return urljoin(self.base_url, url)

        return url

    def is_social_url(self, url: str) -> bool:
        """
        Check if a URL is a social media link

        Args:
            url: URL to check

        Returns:
            bool: True if URL is a social media link
        """
        if not url:
            return False

        url_lower = url.lower()

        # Check against known social domains
        for domain in self.social_domains:
            if domain in url_lower:
                return True

        for pattern in helpers.social_patterns:
            if re.search(pattern, url_lower):
                return True

        return False

    def extract_phone_from_text(self, text: str) -> Optional[str]:
        """
        Extract phone number from text using regex patterns

        Args:
            text: Text to search for phone numbers

        Returns:
            Optional[str]: Extracted phone number or None
        """
        if not text:
            return None

        for pattern in helpers.phone_patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group()
                # Clean up the phone number
                phone = re.sub(r"[^\d\+\-\s\(\)]", "", phone)
                if len(phone) >= 10:  # Minimum valid phone length
                    return phone.strip()

        return None

    def extract_email_from_text(self, text: str) -> Optional[str]:
        """
        Extract email from text using regex

        Args:
            text: Text to search for email

        Returns:
            Optional[str]: Extracted email or None
        """
        if not text:
            return None

        match = re.search(helpers.email_pattern, text)
        if match:
            return match.group()
        return None

    async def extract_company_details(self, page: Page) -> Dict:
        """
        Extract company details from the company detail page

        Args:
            page: Playwright page object

        Returns:
            Dict: Company data
        """
        company_data = {
            "name": None,
            "logo_url": None,
            "description": None,
            "website_url": None,
            "phone": None,
            "email": None,
            "socials": [],
        }

        try:
            # Wait for content to load
            await asyncio.sleep(1)

            # Extract company name
            for selector in helpers.name_selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        name_text = await page.locator(
                            selector
                        ).first.inner_text()
                        if name_text and len(name_text.strip()) > 1:
                            company_data["name"] = name_text.strip()
                            break
                    except:
                        continue

            # Extract logo URL
            for selector in helpers.logo_selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        logo_url = await page.locator(
                            selector
                        ).first.get_attribute("src")
                        if logo_url:
                            company_data["logo_url"] = self.normalize_url(
                                logo_url
                            )
                            break
                    except:
                        continue

            # Extract description
            for selector in helpers.description_selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        if selector.startswith("meta"):
                            description = await page.locator(
                                selector
                            ).first.get_attribute("content")
                        else:
                            description = await page.locator(
                                selector
                            ).first.inner_text()
                        if description and len(description.strip()) > 10:
                            company_data["description"] = description.strip()
                            break
                    except:
                        continue

            # Extract phone number
            phone_found = False
            for selector in helpers.phone_selectors:
                if (
                    await page.locator(selector).count() > 0
                    and not phone_found
                ):
                    try:
                        element = page.locator(selector).first

                        # Try to get from href first
                        phone_href = await element.get_attribute("href")
                        if phone_href and phone_href.startswith("tel:"):
                            company_data["phone"] = phone_href.replace(
                                "tel:", ""
                            ).strip()
                            phone_found = True
                        else:
                            # Get from text content
                            phone_text = await element.inner_text()
                            extracted_phone = self.extract_phone_from_text(
                                phone_text
                            )
                            if extracted_phone:
                                company_data["phone"] = extracted_phone
                                phone_found = True
                    except:
                        continue

            # If no phone found in specific selectors, search in contact sections
            if not phone_found:
                for selector in helpers.contact_selectors:
                    if (
                        await page.locator(selector).count() > 0
                        and not phone_found
                    ):
                        try:
                            contact_text = await page.locator(
                                selector
                            ).first.inner_text()
                            extracted_phone = self.extract_phone_from_text(
                                contact_text
                            )
                            if extracted_phone:
                                company_data["phone"] = extracted_phone
                                phone_found = True
                                break
                        except:
                            continue

            # Extract email
            email_found = False
            for selector in helpers.email_selectors:
                if (
                    await page.locator(selector).count() > 0
                    and not email_found
                ):
                    try:
                        element = page.locator(selector).first

                        # Try to get from href first
                        email_href = await element.get_attribute("href")
                        if email_href and email_href.startswith("mailto:"):
                            company_data["email"] = email_href.replace(
                                "mailto:", ""
                            ).strip()
                            email_found = True
                        else:
                            # Get from text content
                            email_text = await element.inner_text()
                            extracted_email = self.extract_email_from_text(
                                email_text
                            )
                            if extracted_email:
                                company_data["email"] = extracted_email
                                email_found = True
                    except:
                        continue

            # If no email found in specific selectors, search in contact sections
            if not email_found:
                for selector in helpers.contact_selectors:
                    if (
                        await page.locator(selector).count() > 0
                        and not email_found
                    ):
                        try:
                            contact_text = await page.locator(
                                selector
                            ).first.inner_text()
                            extracted_email = self.extract_email_from_text(
                                contact_text
                            )
                            if extracted_email:
                                company_data["email"] = extracted_email
                                email_found = True
                                break
                        except:
                            continue

            # Extract website URL (non-social)
            for selector in helpers.website_selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        elements = page.locator(selector)
                        count = await elements.count()
                        for i in range(count):
                            website_url = await elements.nth(i).get_attribute(
                                "href"
                            )
                            if website_url and "expo" not in website_url:
                                normalized_url = self.normalize_url(
                                    website_url
                                )
                                if normalized_url and not self.is_social_url(
                                    normalized_url
                                ):
                                    # Avoid setting the current page URL as website
                                    current_url = page.url
                                    if (
                                        normalized_url != current_url
                                        and not current_url.startswith(
                                            normalized_url
                                        )
                                    ):
                                        company_data["website_url"] = (
                                            normalized_url
                                        )
                                        break
                        if company_data["website_url"]:
                            break
                    except:
                        continue

            # Extract ALL social media links
            socials = set()

            # Get all links on the page
            all_links = page.locator("a[href]")
            link_count = await all_links.count()

            for i in range(link_count):
                try:
                    href = await all_links.nth(i).get_attribute("href")
                    if href and self.is_social_url(href):
                        normalized_url = self.normalize_url(href)
                        if normalized_url:
                            socials.add(normalized_url)
                except:
                    continue

            # Also check for social links in specific containers
            for selector in helpers.social_container_selectors:
                if await page.locator(selector).count() > 0:
                    try:
                        container_links = page.locator(f"{selector} a[href]")
                        container_count = await container_links.count()
                        for j in range(container_count):
                            href = await container_links.nth(j).get_attribute(
                                "href"
                            )
                            if href and self.is_social_url(href):
                                normalized_url = self.normalize_url(href)
                                if normalized_url:
                                    socials.add(normalized_url)
                    except:
                        continue

            company_data["socials"] = list(socials)

            logger.info(
                f"Extracted company data: {json.dumps(company_data, indent=2)}"
            )

        except Exception as e:
            logger.error(f"Error extracting company details: {str(e)}")

        return company_data

    async def find_company_links(self, page: Page) -> Optional[List[str]]:
        """
        Find all company/exhibitor links on the listing page

        Args:
            page: Playwright page object

        Returns:
            Optional[List[str]]: List of company URLs or None if click-based navigation is needed
        """
        company_urls = []

        # Selectors for company items/links
        found_urls = set()

        for selector in helpers.company_selectors:
            try:
                elements = page.locator(selector)
                count = await elements.count()

                if count > 0:
                    logger.info(
                        f"Found {count} potential company links with selector: {selector}"
                    )

                    for i in range(count):
                        try:
                            href = await elements.nth(i).get_attribute("href")
                            if href:
                                # Filter out navigation/utility links
                                if not any(
                                    pattern in href.lower()
                                    for pattern in helpers.skip_patterns
                                ):
                                    normalized_url = self.normalize_url(href)
                                    if (
                                        normalized_url
                                        and normalized_url not in found_urls
                                    ):
                                        found_urls.add(normalized_url)

                                        # Check if it's likely a detail page
                                        if any(
                                            re.search(pattern, normalized_url)
                                            for pattern in helpers.detail_patterns
                                        ):
                                            company_urls.append(normalized_url)
                        except:
                            continue
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {str(e)}")
                continue

        # If no specific company URLs found, try to identify clickable items
        if not company_urls:
            logger.info(
                "No specific company URLs found, looking for clickable items..."
            )

            # Look for repeated structures that might be company items
            for selector in helpers.item_selectors:
                try:
                    items = page.locator(selector)
                    count = await items.count()

                    # If we find multiple similar items, they might be companies
                    if count >= 5:
                        logger.info(
                            f"Found {count} similar items with selector: {selector}"
                        )
                        # Process these items differently
                        # You might need to click on them instead of following links
                        return None  # Signal to use click-based navigation
                except:
                    continue

        logger.info(f"Found {len(company_urls)} unique company URLs")
        return company_urls

    async def scroll_and_load_more(self, page: Page) -> bool:
        """
        Scroll the page to load more content

        Args:
            page: Playwright page object

        Returns:
            bool: True if new content was loaded
        """
        try:
            # Get initial height
            initial_height = await page.evaluate("document.body.scrollHeight")

            # Try different scroll methods
            for method in helpers.scroll_methods:
                await page.evaluate(method)
                await asyncio.sleep(1)

            # Also try keyboard scrolling
            for _ in range(3):
                await page.keyboard.press("End")
                await asyncio.sleep(0.5)
                await page.keyboard.press("PageDown")
                await asyncio.sleep(0.5)

            # Check for "Load More" or "Show More" buttons
            for selector in helpers.load_more_selectors:
                try:
                    if await page.locator(selector).is_visible():
                        await page.locator(selector).click()
                        await asyncio.sleep(2)
                        logger.info(f"Clicked load more button: {selector}")
                        return True
                except:
                    continue

            # Wait for potential lazy loading
            await asyncio.sleep(2)

            # Check if content height increased
            new_height = await page.evaluate("document.body.scrollHeight")
            return new_height > initial_height

        except Exception as e:
            logger.error(f"Error during scrolling: {str(e)}")
            return False

    async def scrape_companies(self) -> List[Dict]:
        """
        Main scraping function to extract all company data

        Returns:
            List[Dict]: List of company data dictionaries
        """
        async with async_playwright() as p:
            browser: Browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=site-per-process",
                    "--disable-web-security",
                ],
            )

            try:
                context: BrowserContext = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    ignore_https_errors=True,
                )

                page: Page = await context.new_page()
                page.set_default_timeout(self.timeout)

                logger.info(f"Navigating to {self.base_url}...")
                await page.goto(self.base_url)
                await asyncio.sleep(3)

                # Check if we need to handle cookie consent
                cookie_selectors = [
                    'button:has-text("accept")',
                    'button:has-text("agree")',
                    'button:has-text("ok")',
                    '[class*="cookie"] button',
                    '[id*="cookie"] button',
                ]

                for selector in cookie_selectors:
                    try:
                        if await page.locator(selector).is_visible():
                            await page.locator(selector).first.click()
                            logger.info("Accepted cookie consent")
                            await asyncio.sleep(1)
                            break
                    except:
                        continue

                # Try to find company links first
                company_urls = await self.find_company_links(page)

                if company_urls:
                    # Method 1: Navigate to each company URL
                    logger.info(
                        f"Found {len(company_urls)} company URLs to process"
                    )

                    for i, url in enumerate(company_urls, 1):
                        try:
                            logger.info(
                                f"Processing company {i}/{len(company_urls)}: {url}"
                            )

                            # Navigate to company page
                            await page.goto(url)
                            await asyncio.sleep(2)

                            # Extract company data
                            company_data = await self.extract_company_details(
                                page
                            )
                            company_data["source_url"] = url

                            # Save company immediately and check for duplicates
                            self._save_company_immediately(company_data)

                        except Exception as e:
                            logger.error(
                                f"Error processing company {i} ({url}): {str(e)}"
                            )
                            continue

                else:
                    # Method 2: Click-based navigation (for dynamic sites)
                    logger.info("Using click-based navigation method")

                    # Find clickable company items
                    item_selectors = [
                        ".company-item",
                        ".exhibitor-item",
                        ".vendor-item",
                        ".supplier-item",
                        ".company-card",
                        ".exhibitor-card",
                        ".list-item",
                        ".grid-item",
                        ".result-item",
                        ".listing-item",
                        '[class*="company"]',
                        '[class*="exhibitor"]',
                        '[class*="item"]',
                        '[class*="card"]',
                        "article",
                        ".entry",
                        ".result",
                    ]

                    company_elements = None
                    used_selector = None

                    for selector in item_selectors:
                        elements = page.locator(selector)
                        count = await elements.count()
                        if (
                            count >= 3
                        ):  # At least 3 items to be considered a list
                            company_elements = elements
                            used_selector = selector
                            logger.info(
                                f"Found {count} company items with selector: {selector}"
                            )
                            break

                    if company_elements:
                        processed_count = 0
                        scroll_attempts = 0
                        max_scroll_attempts = 4

                        while scroll_attempts < max_scroll_attempts:
                            # Re-locate elements after scrolling
                            if used_selector is not None:
                                company_elements = page.locator(used_selector)
                                current_count = await company_elements.count()
                            else:
                                logger.error(
                                    "No valid selector found for company elements."
                                )
                                break

                            # Process new items
                            for i in range(processed_count, current_count):
                                try:
                                    logger.info(
                                        f"Processing company {i + 1}/{current_count}"
                                    )

                                    # Click on the item
                                    await company_elements.nth(i).click()
                                    await asyncio.sleep(2)

                                    # Check if we navigated to a new page or opened a modal
                                    current_url = page.url
                                    if current_url != self.base_url:
                                        # We navigated to a new page
                                        company_data = (
                                            await self.extract_company_details(
                                                page
                                            )
                                        )
                                        company_data["source_url"] = (
                                            current_url
                                        )

                                        # Save company immediately and check for duplicates
                                        self._save_company_immediately(
                                            company_data
                                        )

                                        # Go back
                                        await page.go_back()
                                        await asyncio.sleep(2)
                                    else:
                                        # Might be a modal or overlay
                                        company_data = (
                                            await self.extract_company_details(
                                                page
                                            )
                                        )
                                        company_data["source_url"] = (
                                            self.base_url
                                        )

                                        # Save company immediately and check for duplicates
                                        self._save_company_immediately(
                                            company_data
                                        )

                                        # Try to close modal/overlay
                                        close_selectors = [
                                            'button[aria-label="Close"]',
                                            "button:has(.close)",
                                            ".close-button",
                                            ".close",
                                            '[class*="close"]',
                                            "button.modal-close",
                                            ".overlay-close",
                                            ".dialog-close",
                                        ]

                                        for close_sel in close_selectors:
                                            try:
                                                if await page.locator(
                                                    close_sel
                                                ).is_visible():
                                                    await page.locator(
                                                        close_sel
                                                    ).click()
                                                    await asyncio.sleep(1)
                                                    break
                                            except:
                                                continue

                                        # If no close button, try ESC key
                                        await page.keyboard.press("Escape")
                                        await asyncio.sleep(1)

                                except Exception as e:
                                    logger.error(
                                        f"Error processing company {i + 1}: {str(e)}"
                                    )
                                    # Try to recover
                                    try:
                                        if page.url != self.base_url:
                                            await page.goto(self.base_url)
                                            await asyncio.sleep(2)
                                    except:
                                        pass
                                    continue

                            processed_count = current_count

                            # Try to load more content
                            if not await self.scroll_and_load_more(page):
                                scroll_attempts += 1
                                if scroll_attempts >= max_scroll_attempts:
                                    logger.info("No more content to load")
                                    break
                            else:
                                scroll_attempts = (
                                    0  # Reset if new content loaded
                                )

                            await asyncio.sleep(2)

                    else:
                        # Method 3: Extract all data from the current page
                        logger.info("Extracting data from current page")
                        company_data = await self.extract_company_details(page)
                        company_data["source_url"] = self.base_url

                        # Save company immediately and check for duplicates
                        self._save_company_immediately(company_data)

            finally:
                await browser.close()

        logger.info(
            f"Scraping completed. Extracted data for {len(self.companies_data)} companies"
        )
        return self.companies_data

    def save_to_csv(self, filename: Optional[str] = None):
        """
        Re-export all data to CSV file (companies are already saved incrementally)
        This method is mainly for creating a fresh export or custom filename

        Args:
            filename: Optional custom filename. If not provided, uses domain_companies.csv
        """
        if not filename:
            filename = f"{self.domain}_companies.csv"

        if not self.companies_data:
            logger.warning("No data to save")
            return

        try:
            # Prepare CSV data with separate social media columns
            fieldnames = [
                "company_index",
                "name",
                "description",
                "website_url",
                "phone",
                "email",
                "logo_url",
                "source_url",
                "facebook",
                "instagram",
                "linkedin",
                "twitter",
                "other_socials",
            ]

            # Write to CSV
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for company in self.companies_data:
                    # Prepare company data for CSV
                    company_copy = company.copy()

                    # Categorize social media links
                    socials = company.get("socials", [])
                    social_categories = self._categorize_social_media(socials)

                    # Add categorized social media to company data
                    company_copy.update(social_categories)

                    # Remove the original socials field for CSV
                    if "socials" in company_copy:
                        del company_copy["socials"]

                    # Ensure all fields exist and handle None values
                    for field in fieldnames:
                        if (
                            field not in company_copy
                            or company_copy[field] is None
                        ):
                            company_copy[field] = ""

                    writer.writerow(company_copy)

            logger.info(f"Data re-exported to {filename}")

            # Also save as JSON for complete data
            json_filename = filename.replace(".csv", ".json")
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(self.companies_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Complete data also re-exported to {json_filename}")

        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")

    def get_scraping_stats(self) -> Dict:
        """
        Get statistics about the current scraping session

        Returns:
            Dict: Statistics about scraped companies
        """
        # Calculate social media statistics
        facebook_count = 0
        instagram_count = 0
        linkedin_count = 0
        twitter_count = 0
        other_socials_count = 0

        for company in self.companies_data:
            socials = company.get("socials", [])
            if socials:
                social_categories = self._categorize_social_media(socials)
                if social_categories["facebook"]:
                    facebook_count += 1
                if social_categories["instagram"]:
                    instagram_count += 1
                if social_categories["linkedin"]:
                    linkedin_count += 1
                if social_categories["twitter"]:
                    twitter_count += 1
                if social_categories["other_socials"]:
                    other_socials_count += 1

        return {
            "total_companies": len(self.companies_data),
            "companies_with_name": sum(
                1 for c in self.companies_data if c.get("name")
            ),
            "companies_with_description": sum(
                1 for c in self.companies_data if c.get("description")
            ),
            "companies_with_website": sum(
                1 for c in self.companies_data if c.get("website_url")
            ),
            "companies_with_phone": sum(
                1 for c in self.companies_data if c.get("phone")
            ),
            "companies_with_email": sum(
                1 for c in self.companies_data if c.get("email")
            ),
            "companies_with_logo": sum(
                1 for c in self.companies_data if c.get("logo_url")
            ),
            "companies_with_socials": sum(
                1 for c in self.companies_data if c.get("socials")
            ),
            "companies_with_facebook": facebook_count,
            "companies_with_instagram": instagram_count,
            "companies_with_linkedin": linkedin_count,
            "companies_with_twitter": twitter_count,
            "companies_with_other_socials": other_socials_count,
            "csv_file": self.csv_filename,
            "json_file": self.json_filename,
        }

    def clear_all_data(self):
        """
        Clear all scraped data and delete files (use with caution!)
        """
        try:
            self.companies_data = []
            self.processed_companies = set()

            # Delete files if they exist
            if Path(self.csv_filename).exists():
                Path(self.csv_filename).unlink()
                logger.info(f"Deleted {self.csv_filename}")

            if Path(self.json_filename).exists():
                Path(self.json_filename).unlink()
                logger.info(f"Deleted {self.json_filename}")

            logger.info("All data cleared")

        except Exception as e:
            logger.error(f"Error clearing data: {str(e)}")

    def print_summary(self):
        """
        Print summary statistics of the scraped data
        """
        if not self.companies_data:
            print("No data collected")
            return

        print(f"\n{'='*60}")
        print(f"SCRAPING SUMMARY FOR {self.domain}")
        print(f"{'='*60}")
        print(f"Total companies processed: {len(self.companies_data)}")
        print(
            f"Companies with name: {sum(1 for c in self.companies_data if c.get('name'))}"
        )
        print(
            f"Companies with description: {sum(1 for c in self.companies_data if c.get('description'))}"
        )
        print(
            f"Companies with website: {sum(1 for c in self.companies_data if c.get('website_url'))}"
        )
        print(
            f"Companies with phone: {sum(1 for c in self.companies_data if c.get('phone'))}"
        )
        print(
            f"Companies with email: {sum(1 for c in self.companies_data if c.get('email'))}"
        )
        print(
            f"Companies with logo: {sum(1 for c in self.companies_data if c.get('logo_url'))}"
        )
        print(
            f"Companies with social media: {sum(1 for c in self.companies_data if c.get('socials'))}"
        )

        # Social media platform distribution
        if self.companies_data:
            all_social_platforms = {}
            for company in self.companies_data:
                for social_url in company.get("socials", []):
                    # Extract platform name from URL
                    social_url_lower = social_url.lower()
                    for domain in self.social_domains:
                        if domain in social_url_lower:
                            platform = domain.split(".")[0]
                            all_social_platforms[platform] = (
                                all_social_platforms.get(platform, 0) + 1
                            )
                            break

            if all_social_platforms:
                print(f"\n{'='*60}")
                print("SOCIAL MEDIA PLATFORMS DISTRIBUTION")
                print(f"{'='*60}")
                sorted_platforms = sorted(
                    all_social_platforms.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )
                for platform, count in sorted_platforms[:10]:  # Show top 10
                    print(f"{platform:20} : {count:4} companies")
                if len(sorted_platforms) > 10:
                    print(
                        f"... and {len(sorted_platforms) - 10} more platforms"
                    )

        # Sample data
        if self.companies_data:
            print(f"\n{'='*60}")
            print("SAMPLE DATA (First 3 companies)")
            print(f"{'='*60}")
            for i, company in enumerate(self.companies_data[:3], 1):
                print(f"\n--- Company {i} ---")
                for key, value in company.items():
                    if key == "socials" and value:
                        print(f"{key}: {len(value)} social links")
                        for social in value[:3]:  # Show first 3 social links
                            print(f"  - {social}")
                        if len(value) > 3:
                            print(f"  ... and {len(value) - 3} more")
                    elif key == "description" and value:
                        # Truncate long descriptions
                        desc = (
                            value[:200] + "..." if len(value) > 200 else value
                        )
                        print(f"{key}: {desc}")
                    else:
                        print(f"{key}: {value}")
