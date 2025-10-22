"""
JobToday Scraper with n8n Cloud Webhook Integration
Sends results directly to n8n Cloud after scraping
"""
import asyncio
import json
import csv
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import os
from dotenv import load_dotenv
import requests

load_dotenv()

class JobTodayWebhookScraper:
    def __init__(self):
        self.email = os.getenv('JOBTODAY_EMAIL')
        self.password = os.getenv('JOBTODAY_PASSWORD')
        if not self.email or not self.password:
            raise ValueError("JOBTODAY_EMAIL and JOBTODAY_PASSWORD must be set in the .env file")
            
        self.job_id = "p3j9ox" 
        self.base_url = "https://web.jobtoday.com"
        self.candidates = []
        self.processed_names = set()
        self.playwright = None
        self.browser = None
        self.job_role = None
        
        # Airtable setup
        self.airtable_token = os.getenv('AIRTABLE_PAT')
        self.airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
        self.airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME', 'Candidates')
        self.airtable_api_url = f"https://api.airtable.com/v0/{self.airtable_base_id}/{self.airtable_table_name}"
        
        # n8n Cloud Webhook URL
        self.n8n_webhook_url = os.getenv('N8N_WEBHOOK_URL')
        
        if not self.airtable_token or not self.airtable_base_id:
            print("⚠ Airtable credentials not fully configured")
        else:
            print("✓ Airtable configured")
            
        if not self.n8n_webhook_url:
            print("⚠ N8N_WEBHOOK_URL not set - webhook notifications disabled")
        else:
            print("✓ n8n webhook configured")
        
    async def initialize_browser(self, headless=True):
        """Initialize browser with Render-compatible settings"""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("Starting Playwright...")
            self.playwright = await async_playwright().start()
            
            logger.info("Launching Chromium browser...")
            
            # Render-compatible browser arguments
            browser_args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920x1080',
                '--single-process',
                '--disable-dev-tools',
                '--no-zygote',
                '--disable-blink-features=AutomationControlled'
            ]
            
            logger.info(f"Browser args: {browser_args}")
            
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                args=browser_args
            )
            
            logger.info("Creating browser context...")
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            await self.context.grant_permissions(['geolocation'], origin=self.base_url)
            
            logger.info("Creating new page...")
            self.page = await self.context.new_page()
            
            logger.info("✓ Browser initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Browser initialization failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
    async def login(self):
        try:
            print("→ Navigating to JobToday to log in...")
            await self.page.goto(f"{self.base_url}/auth/login", wait_until='domcontentloaded', timeout=60000)
            
            # Wait a bit for any redirects or dynamic content
            await asyncio.sleep(3)
            
            # Check if already logged in by looking for multiple indicators
            post_login_selectors = [
                '[data-testid="tabs-my_jobs"]',
                'a[href="/jobs"]',
                'button:has-text("Post a job")',
                '[data-testid="sidebar"]'
            ]
            
            # Try to find any post-login element
            already_logged_in = False
            for selector in post_login_selectors:
                try:
                    element = await self.page.wait_for_selector(selector, timeout=5000, state='attached')
                    if element:
                        # Check if element is actually visible in viewport
                        is_visible = await element.is_visible()
                        if is_visible:
                            print("✓ Already logged in (found visible post-login element).")
                            already_logged_in = True
                            break
                except:
                    continue
            
            if already_logged_in:
                return True
                
            print("→ Not logged in. Proceeding with login...")
            
            # Wait for login form
            try:
                await self.page.wait_for_selector('input[type="email"]', timeout=10000, state='visible')
            except:
                # Maybe we're already logged in but elements were hidden
                current_url = self.page.url
                if '/auth/login' not in current_url:
                    print("✓ Already logged in (redirected away from login page).")
                    return True
                raise Exception("Login form not found")
            
            # Fill in credentials
            email_input = self.page.locator('input[type="email"]')
            password_input = self.page.locator('input[type="password"]')
            
            await email_input.wait_for(state='visible', timeout=30000)
            print("→ Filling in email...")
            await email_input.fill(self.email)
            await asyncio.sleep(1)
            
            print("→ Filling in password...")
            await password_input.fill(self.password)
            await asyncio.sleep(1)
            
            print("→ Clicking submit button...")
            submit_button = self.page.locator('button[type="submit"]')
            await submit_button.click()
            
            # Wait for navigation after login
            print("→ Waiting for login to complete...")
            
            # Wait for URL to change (indicates successful login)
            try:
                await self.page.wait_for_url(lambda url: '/auth/login' not in url, timeout=30000)
                print("   ✓ URL changed, login appears successful")
            except:
                print("   ⚠ URL didn't change, checking for other indicators...")
            
            # Wait for any post-login element with a longer timeout
            await asyncio.sleep(5)
            
            # Check multiple ways to confirm login
            login_confirmed = False
            
            # Method 1: Check URL
            if '/auth/login' not in self.page.url:
                print("   ✓ Redirected away from login page")
                login_confirmed = True
            
            # Method 2: Check for post-login elements (visible or not)
            if not login_confirmed:
                for selector in post_login_selectors:
                    try:
                        count = await self.page.locator(selector).count()
                        if count > 0:
                            print(f"   ✓ Found post-login element: {selector}")
                            login_confirmed = True
                            break
                    except:
                        continue
            
            # Method 3: Check if login form is gone
            if not login_confirmed:
                login_form_count = await self.page.locator('input[type="email"]').count()
                if login_form_count == 0:
                    print("   ✓ Login form disappeared")
                    login_confirmed = True
            
            if login_confirmed:
                print("✓ Login successful")
                await asyncio.sleep(3)
                return True
            else:
                # Don't take screenshot - just log the error
                print(f"✗ Could not confirm successful login")
                print(f"   Current URL: {self.page.url}")
                raise Exception("Could not confirm successful login")
            
        except Exception as e:
            print(f"✗ Login failed: {e}")
            # Don't take screenshot in production - it's too slow
            print(f"   Current URL: {self.page.url if hasattr(self, 'page') and self.page else 'N/A'}")
            return False
        

    async def login_with_retry(self, max_attempts=3):
        """Try to login with retries"""
        for attempt in range(1, max_attempts + 1):
            print(f"\n→ Login attempt {attempt}/{max_attempts}")
            
            success = await self.login()
            if success:
                return True
            
            if attempt < max_attempts:
                print(f"   Waiting 10 seconds before retry...")
                await asyncio.sleep(10)
        
        return False
    
    async def save_session(self, filename='session.json'):
        """Save browser session/cookies"""
        try:
            cookies = await self.context.cookies()
            storage = await self.context.storage_state()
            
            session_data = {
                'cookies': cookies,
                'storage': storage
            }
            
            with open(filename, 'w') as f:
                json.dump(session_data, f)
            print(f"✓ Session saved to {filename}")
        except Exception as e:
            print(f"⚠ Could not save session: {e}")

    async def load_session(self, filename='session.json'):
        """Load browser session/cookies"""
        try:
            if not os.path.exists(filename):
                return False
            
            with open(filename, 'r') as f:
                session_data = json.load(f)
            
            await self.context.add_cookies(session_data['cookies'])
            print(f"✓ Session loaded from {filename}")
            return True
        except Exception as e:
            print(f"⚠ Could not load session: {e}")
            return False
        
    async def scrape_job_role(self):
        """Scrape the job role from the main job page"""
        try:
            print("\n→ Scraping job role...")
            main_job_url = f"{self.base_url}/jobs/{self.job_id}"
            await self.page.goto(main_job_url, wait_until='domcontentloaded', timeout=90000)  # Increased
            await asyncio.sleep(5)  # Increased wait time
            
            # Wait for the role element and extract it
            role_selector = 'div.bg-white.rounded-b-xl div.text-black.font-bold.mb-1'
            await self.page.wait_for_selector(role_selector, timeout=20000)  # Increased
            
            role_element = self.page.locator(role_selector).first
            self.job_role = await role_element.inner_text()
            
            print(f"   ✓ Job role scraped: {self.job_role}")
            return self.job_role
            
        except Exception as e:
            print(f"   ⚠ Could not scrape job role: {e}")
            # Set a default role instead of failing
            self.job_role = f"Job {self.job_id}"
            print(f"   → Using default role: {self.job_role}")
            return self.job_role
        
    async def safe_goto(self, url, timeout=90000, max_retries=3):
        """Navigate to URL with retries and better error handling"""
        for attempt in range(1, max_retries + 1):
            try:
                print(f"   → Navigating to {url} (attempt {attempt}/{max_retries})...")
                await self.page.goto(url, wait_until='domcontentloaded', timeout=timeout)
                await asyncio.sleep(3)
                
                # Verify we didn't get redirected to login
                if '/auth/login' in self.page.url:
                    print("   ⚠ Got redirected to login, re-authenticating...")
                    if not await self.login_with_retry(max_attempts=2):
                        raise Exception("Re-login failed")
                    # Try again after login
                    await self.page.goto(url, wait_until='domcontentloaded', timeout=timeout)
                    await asyncio.sleep(3)
                
                print(f"   ✓ Successfully navigated")
                return True
                
            except Exception as e:
                print(f"   ⚠ Navigation attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    wait_time = attempt * 5  # Exponential backoff
                    print(f"   → Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"   ✗ All navigation attempts failed")
                    raise
        
        return False

    async def safe_click(self, locator, timeout=45000, max_retries=2):
        """Click element with retries"""
        for attempt in range(1, max_retries + 1):
            try:
                await locator.click(timeout=timeout)
                return True
            except Exception as e:
                if attempt < max_retries:
                    print(f"   ⚠ Click attempt {attempt} failed, retrying...")
                    await asyncio.sleep(3)
                else:
                    raise
        return False

    async def scrape_section(self, section_name):
        section_url = f"{self.base_url}/jobs/{self.job_id}/{section_name}"
        print(f"\n→ Processing section: {section_url}")
        
        # Update progress
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(section=section_name)
        
        max_page_load_attempts = 3
        page_loaded = False
        
        for attempt in range(1, max_page_load_attempts + 1):
            try:
                print(f"   → Loading page (attempt {attempt}/{max_page_load_attempts})...")
                await self.page.goto(section_url, wait_until='domcontentloaded', timeout=90000)
                
                # Check if we got redirected to login
                await asyncio.sleep(5)
                current_url = self.page.url
                
                if '/auth/login' in current_url:
                    print("   ⚠ Session expired, re-authenticating...")
                    if not await self.login_with_retry(max_attempts=2):
                        raise Exception("Re-login failed")
                    await self.page.goto(section_url, wait_until='domcontentloaded', timeout=90000)
                    await asyncio.sleep(5)
                
                # Wait for the list container
                list_container_selector = 'div.col-span-1.overflow-y-auto'
                
                try:
                    await self.page.wait_for_selector(list_container_selector, timeout=45000)
                    print("   ✓ List container found")
                    page_loaded = True
                    break
                except PlaywrightTimeout:
                    print(f"   ⚠ List container not found (attempt {attempt})")
                    
                    # Try alternative selectors
                    alt_selectors = [
                        'div.overflow-y-auto',
                        '[class*="overflow-y-auto"]',
                        'div[class*="col-span"]'
                    ]
                    
                    for alt_selector in alt_selectors:
                        if await self.page.locator(alt_selector).count() > 0:
                            print(f"   → Found alternative container: {alt_selector}")
                            list_container_selector = alt_selector
                            page_loaded = True
                            break
                    
                    if page_loaded:
                        break
                        
                    if attempt < max_page_load_attempts:
                        print(f"   → Retrying page load...")
                        await asyncio.sleep(10)
                        
            except Exception as e:
                print(f"   ✗ Error loading page (attempt {attempt}): {e}")
                if attempt < max_page_load_attempts:
                    await asyncio.sleep(10)
                else:
                    raise
        
        if not page_loaded:
            raise Exception(f"Could not load section {section_name} after {max_page_load_attempts} attempts")
        
        # Continue with scraping
        await asyncio.sleep(7)
        
        # Count candidates
        candidate_button_selector = f'button:has(img[alt$="\'s avatar"])'
        
        print("   → Waiting for candidates to load...")
        await asyncio.sleep(5)
        
        try:
            total_candidates_in_section = await self.page.locator(candidate_button_selector).count()
        except Exception as e:
            print(f"   ✗ Error counting candidates: {e}")
            total_candidates_in_section = 0
        
        # Update progress with total
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(total=total_candidates_in_section)

        if total_candidates_in_section == 0:
            print(f"   ! No candidates found in the '{section_name}' section.")
            return

        print(f"   ✓ Found {total_candidates_in_section} candidates.")

        # Process candidates one by one
        for i in range(total_candidates_in_section):
            candidate_name = f"Candidate {i+1}"
            
            try:
                print(f"\n--- Starting candidate {i+1}/{total_candidates_in_section} ---")
                
                # Update heartbeat before processing each candidate
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(
                        candidate=f"Loading candidate {i+1}",
                        processed=i
                    )
                
                # Re-check if page is still valid
                if self.page.is_closed():
                    print("   ! Browser has closed unexpectedly. Halting.")
                    return
                
                # Try to get candidate button with timeout
                try:
                    await self.page.wait_for_selector(candidate_button_selector, timeout=30000)
                except PlaywrightTimeout:
                    print(f"   ⚠ Candidate buttons not found, may have navigated away. Returning to list...")
                    await self.page.goto(section_url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(5)
                    await self.page.wait_for_selector(candidate_button_selector, timeout=30000)
                
                candidate_button = self.page.locator(candidate_button_selector).nth(i)
                
                # Get candidate name
                try:
                    candidate_name = await candidate_button.locator('.font-bold').first.inner_text(timeout=10000)
                    print(f"   → Candidate name: {candidate_name}")
                except Exception as e:
                    print(f"   ⚠ Could not get candidate name: {e}")
                    candidate_name = f"Candidate {i+1}"
                
                # Check if already processed
                if candidate_name in self.processed_names:
                    print(f"   ✓ Skipping (already processed)")
                    continue
                
                # Update progress with candidate name
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(candidate=candidate_name)
                
                # Get application date
                application_date = "N/A"
                try:
                    application_date_locator = candidate_button.locator('p:has-text("Applied on")')
                    if await application_date_locator.count() > 0:
                        application_date = await application_date_locator.inner_text(timeout=5000)
                except Exception as e:
                    print(f"   ⚠ Could not get application date: {e}")
                
                # Click candidate button
                print(f"   → Clicking candidate button...")
                try:
                    await candidate_button.click(timeout=30000)
                    print(f"   ✓ Clicked")
                except Exception as e:
                    print(f"   ✗ Failed to click: {e}")
                    continue
                
                # Wait for profile to load
                profile_view_selector = 'button:has-text("Chat with")'
                print("   → Waiting for profile details to load...")
                
                try:
                    await self.page.wait_for_selector(profile_view_selector, timeout=45000)
                    print("   ✓ Profile details loaded")
                except PlaywrightTimeout:
                    print(f"   ✗ Profile did not load within timeout")
                    # Try to go back to list
                    await self.page.goto(section_url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(3)
                    continue
                
                # Remove popups
                try:
                    await self.page.evaluate('document.querySelectorAll("[id^=intercom-container], .intercom-lightweight-app").forEach(el => el.remove())')
                except:
                    pass
                
                await asyncio.sleep(2)
                
                # Scrape details
                print(f"   → Scraping candidate details...")
                try:
                    details = await self.scrape_candidate_details(self.page.url, application_date)
                    self.candidates.append(details)
                    self.processed_names.add(candidate_name)
                    print(f"   ✓ Scraped: {details.get('name', 'N/A')}")
                    
                    # Update progress after successful scrape
                    if hasattr(self, 'progress_tracker'):
                        self.progress_tracker.update(processed=i+1)
                        
                except Exception as e:
                    print(f"   ✗ Error scraping details: {e}")
                    import traceback
                    print(f"   Traceback: {traceback.format_exc()[:500]}")
                
                # Return to list view for next candidate
                print(f"   ← Returning to list view...")
                try:
                    await self.page.goto(section_url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(3)
                    print(f"   ✓ Back at list view")
                except Exception as e:
                    print(f"   ✗ Error returning to list: {e}")
                    # Try to continue anyway
                    await asyncio.sleep(5)
            
            except Exception as e:
                print(f"   ✗ Unexpected error processing candidate {i+1}: {e}")
                import traceback
                print(f"   Traceback: {traceback.format_exc()[:500]}")
                
                # Try to recover by going back to list
                try:
                    if not self.page.is_closed():
                        await self.page.goto(section_url, wait_until="domcontentloaded", timeout=90000)
                        await asyncio.sleep(3)
                except:
                    print(f"   ✗ Could not recover, stopping section scraping")
                    break
        
        print(f"\n✓ Finished processing section: {section_name}")
        print(f"   Total candidates found: {total_candidates_in_section}")
        print(f"   Successfully scraped: {len([c for c in self.candidates if c.get('name') != 'N/A'])}")

    async def scrape_candidate_details(self, candidate_url, application_date):
        details = {
            'profile_url': candidate_url, 
            'application_date': application_date, 
            'job_role': self.job_role
        }
        
        try:
            profile_pane = self.page.locator('div.col-span-1.overflow-y-auto:has(button:has-text("Chat with"))')

            async def get_text(locator, timeout=5000):
                try: 
                    await locator.wait_for(timeout=timeout, state='attached')
                    text = await locator.inner_text(timeout=timeout)
                    return text if text else "N/A"
                except Exception as e:
                    return "N/A"

            # Get name
            print(f"      → Getting name...")
            details['name'] = await get_text(profile_pane.locator('div.font-bold.text-2xl').first)
            
            # Get phone number
            print(f"      → Getting phone...")
            try:
                phone_container = profile_pane.locator('div.flex.items-center.gap-2.mt-2:has(img[src*="IconPhoneFilled28"])')
                show_phone_button = profile_pane.locator('span.cursor-pointer.text-jt-blue-500:has-text("Show phone")')
                
                phone_number = "N/A"
                
                if await show_phone_button.count() > 0:
                    try:
                        await show_phone_button.click(timeout=5000)
                        await asyncio.sleep(2)
                    except:
                        pass
                    
                    # Try to get phone number
                    for attempt in range(3):
                        try:
                            phone_span = phone_container.locator('span').first
                            phone_text = await phone_span.text_content(timeout=3000)
                            phone_number = phone_text.strip() if phone_text else ""
                            
                            if "Show phone" in phone_number:
                                phone_number = phone_number.replace("Show phone", "").strip()
                            
                            if phone_number and '…' not in phone_number and len(phone_number) >= 9:
                                break
                            
                            await asyncio.sleep(1)
                        except:
                            break
                else:
                    phone_span = phone_container.locator('span').first
                    phone_text = await phone_span.text_content(timeout=3000)
                    phone_number = phone_text.strip() if phone_text else "N/A"
                
                details['phone'] = phone_number
                print(f"      ✓ Phone: {phone_number}")
                        
            except Exception as e: 
                print(f"      ⚠ Error getting phone: {str(e)[:100]}")
                details['phone'] = "N/A"
            
            # Get email
            print(f"      → Getting email...")
            details['email'] = await get_text(profile_pane.locator('a[href^="mailto:"]').first)
            
            # Get location
            print(f"      → Getting location...")
            details['location'] = await get_text(profile_pane.locator('div:has(img[src*="IconPinThinBlack20"]) > span').first)
            
            # Get about
            print(f"      → Getting about...")
            try:
                about_text_div = profile_pane.locator('hr.my-6 + div.px-4.break-word').first
                details['about'] = await get_text(about_text_div)
            except Exception:
                details['about'] = "N/A"
            
            # Get certificates
            print(f"      → Getting certificates...")
            try:
                certs_header = profile_pane.locator('div.font-bold.text-xl:has-text("Certificates")').first
                certs_block = certs_header.locator('xpath=./following-sibling::div[1]')
                details['certificates'] = await get_text(certs_block)
            except Exception:
                details['certificates'] = "N/A"
                
            # Get experience
            print(f"      → Getting experience...")
            try:
                exp_header = profile_pane.locator('div.font-bold.text-xl:has-text("Experience")').first
                experience_block = exp_header.locator('xpath=./following-sibling::div[1]')
                details['experience'] = await get_text(experience_block)
            except Exception:
                details['experience'] = "N/A"
                
            # Get languages
            print(f"      → Getting languages...")
            try:
                lang_header = profile_pane.locator('div.font-bold.text-xl:has-text("Languages")').first
                languages_block = lang_header.locator('xpath=./following-sibling::div[1]')
                details['languages'] = await get_text(languages_block)
            except Exception:
                details['languages'] = "N/A"

            print(f"      ✓ All details scraped")
            return details
            
        except Exception as e:
            print(f"      ✗ Critical error in scrape_candidate_details: {e}")
            import traceback
            print(f"      Traceback: {traceback.format_exc()[:500]}")
            # Return partial details
            return details

    def get_existing_profiles(self):
        """Fetch all existing profile URLs from Airtable to check for duplicates"""
        if not self.airtable_token or not self.airtable_base_id:
            return set()
            
        try:
            print("→ Checking for existing candidates in Airtable...")
            headers = {
                'Authorization': f'Bearer {self.airtable_token}'
            }
            
            existing_urls = set()
            offset = None
            
            while True:
                params = {
                    'fields[]': 'Profile URL',
                    'pageSize': 100
                }
                
                if offset:
                    params['offset'] = offset
                
                response = requests.get(
                    self.airtable_api_url, 
                    headers=headers, 
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    print(f"   ⚠ Could not fetch existing records: {response.status_code}")
                    print(f"      Response: {response.text}")
                    return set()
                
                data = response.json()
                for record in data.get('records', []):
                    profile_url = record.get('fields', {}).get('Profile URL', '')
                    if profile_url:
                        existing_urls.add(profile_url)
                
                offset = data.get('offset')
                if not offset:
                    break
            
            print(f"   ✓ Found {len(existing_urls)} existing candidates in Airtable")
            return existing_urls
            
        except Exception as e:
            print(f"   ✗ Error fetching existing profiles: {e}")
            return set()

    def push_to_airtable(self):
        """Push only new candidate data to Airtable (skip duplicates)"""
        if not self.airtable_token or not self.airtable_base_id:
            print("⚠ Airtable credentials missing. Skipping Airtable update.")
            return []
            
        try:
            print("\n→ Pushing data to Airtable...")
            
            existing_urls = self.get_existing_profiles()
            
            new_candidates = [
                c for c in self.candidates 
                if c.get('profile_url') not in existing_urls
            ]
            
            if not new_candidates:
                print("   ! All candidates already exist in Airtable. Nothing to push.")
                return []
            
            print(f"   → Found {len(new_candidates)} new candidates to push (skipped {len(self.candidates) - len(new_candidates)} duplicates)")
            
            headers = {
                'Authorization': f'Bearer {self.airtable_token}',
                'Content-Type': 'application/json'
            }
            
            records = []
            for candidate in new_candidates:
                record = {
                    'fields': {
                        'Name': candidate.get('name', ''),
                        'Phone': candidate.get('phone', ''),
                        'Email': candidate.get('email', ''),
                        'Location': candidate.get('location', ''),
                        'About': candidate.get('about', ''),
                        'Experience': candidate.get('experience', ''),
                        'Languages': candidate.get('languages', ''),
                        'Certificates': candidate.get('certificates', ''),
                        'Profile URL': candidate.get('profile_url', ''),
                        'Application Date': candidate.get('application_date', ''),
                        'Role': candidate.get('job_role', ''),  
                        'Status': 'New',
                        'Notes': ''
                    }
                }
                records.append(record)
            
            batch_size = 10
            total_pushed = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                payload = {'records': batch}
                
                response = requests.post(
                    self.airtable_api_url,
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    total_pushed += len(batch)
                    print(f"   ✓ Pushed batch {i//batch_size + 1} ({len(batch)} records)")
                else:
                    print(f"   ✗ Error pushing batch {i//batch_size + 1}: {response.status_code}")
                    print(f"      Response: {response.text}")
            
            print(f"✓ Successfully pushed {total_pushed} new candidates to Airtable")
            return new_candidates
            
        except Exception as e:
            print(f"✗ Error pushing to Airtable: {e}")
            return []

    def send_to_n8n_webhook(self, new_candidates):
        """Send new candidates to n8n Cloud webhook"""
        if not self.n8n_webhook_url:
            print("⚠ n8n webhook URL not configured. Skipping webhook notification.")
            return
            
        try:
            print("\n→ Sending data to n8n Cloud webhook...")
            
            payload = {
                'timestamp': datetime.now().isoformat(),
                'job_id': self.job_id,
                'total_scraped': len(self.candidates),
                'new_candidates_count': len(new_candidates),
                'new_candidates': new_candidates,
                'status': 'success'
            }
            
            response = requests.post(
                self.n8n_webhook_url,
                json=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 204]:
                print(f"✓ Successfully sent {len(new_candidates)} new candidates to n8n")
            else:
                print(f"⚠ Webhook returned status {response.status_code}")
                print(f"   Response: {response.text}")
                
        except Exception as e:
            print(f"✗ Error sending to n8n webhook: {e}")

    async def save_to_json(self, filename='candidates_detailed.json'):
        output = {
            'scraped_at': datetime.now().isoformat(),
            'job_id': self.job_id,
            'total_candidates': len(self.candidates),
            'candidates': self.candidates
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        print(f"✓ Data successfully saved to {filename}")

    async def export_to_csv(self, filename='candidates_detailed.csv'):
        if not self.candidates:
            return
        try:
            fieldnames = set()
            for c in self.candidates:
                fieldnames.update(c.keys())
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(list(fieldnames)))
                writer.writeheader()
                writer.writerows(self.candidates)
            print(f"✓ Data successfully exported to {filename}")
        except PermissionError:
            print(f"✗ ERROR: Permission denied when writing to {filename}.")
        except Exception as e:
            print(f"✗ An unexpected error occurred during CSV export: {e}")

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        print("✓ Browser closed.")

    async def run(self, headless=True):
        try:
            await self.initialize_browser(headless=headless)
            
            # Try to load existing session
            session_loaded = await self.load_session()
            
            if not session_loaded:
                # No session, need to login
                if not await self.login_with_retry(max_attempts=3):
                    print("✗ Halting execution due to login failure.")
                    return
                # Save session for next time
                await self.save_session()
            else:
                print("→ Session loaded, verifying login status...")
                # Verify we're still logged in
                await self.page.goto(self.base_url, wait_until='domcontentloaded')
                await asyncio.sleep(3)
                
                # Check if we need to re-login
                if '/auth/login' in self.page.url:
                    print("   Session expired, logging in again...")
                    if not await self.login_with_retry(max_attempts=3):
                        print("✗ Halting execution due to login failure.")
                        return
                    await self.save_session()
                else:
                    print("   ✓ Session still valid!")

            await self.scrape_job_role()

            print("\n→ Navigating to the applicants section...")

            # Direct navigation to incoming section (more reliable)
            incoming_url = f"{self.base_url}/jobs/{self.job_id}/incoming"

            try:
                # Try direct navigation first (fastest method)
                print("   → Attempting direct navigation to applicants...")
                await self.page.goto(incoming_url, wait_until='domcontentloaded', timeout=45000)
                
                # Wait for the candidates list to load
                await self.page.wait_for_selector('div.col-span-1.overflow-y-auto', timeout=30000)
                print("   ✓ Successfully navigated to applicants area (direct).")
                
            except Exception as direct_nav_error:
                print(f"   ⚠ Direct navigation failed: {direct_nav_error}")
                print("   → Trying navigation via main job page...")
                
                # Fallback: Navigate via main job page
                try:
                    main_job_url = f"{self.base_url}/jobs/{self.job_id}"
                    await self.page.goto(main_job_url, wait_until='domcontentloaded', timeout=45000)
                    
                    # Wait for page to fully load
                    await asyncio.sleep(3)
                    
                    # Multiple possible selectors for the applicants link
                    applicants_selectors = [
                        f'a[href="/jobs/{self.job_id}/incoming"]',
                        f'a[href*="/jobs/{self.job_id}/incoming"]',
                        'a:has-text("Applicants")',
                        'a:has-text("applicants")'
                    ]
                    
                    clicked = False
                    for selector in applicants_selectors:
                        try:
                            print(f"   → Trying selector: {selector}")
                            await self.page.wait_for_selector(selector, timeout=10000, state='visible')
                            await self.page.click(selector)
                            clicked = True
                            print(f"   ✓ Clicked using selector: {selector}")
                            break
                        except:
                            continue
                    
                    if not clicked:
                        # Last resort: use direct navigation
                        print("   → No link found, using direct URL navigation...")
                        await self.page.goto(incoming_url, wait_until='domcontentloaded', timeout=45000)
                    
                    # Wait for candidates list
                    await self.page.wait_for_selector('div.col-span-1.overflow-y-auto', timeout=30000)
                    print("   ✓ Successfully navigated to applicants area (via fallback).")
                    
                except Exception as fallback_error:
                    print(f"   ✗ All navigation methods failed: {fallback_error}")
                    raise Exception("Could not navigate to applicants section")

            sections_to_scrape = ['recommended', 'incoming']
            for section in sections_to_scrape:
                if self.page.is_closed():
                    break
                await self.scrape_section(section)

            if not self.candidates:
                print("\n✗ Scraping finished, but no candidate data was extracted.")
                return

            await self.save_to_json()
            await self.export_to_csv()
            
            # Push to Airtable and get new candidates
            new_candidates = self.push_to_airtable()
            
            # Send to n8n webhook
            self.send_to_n8n_webhook(new_candidates)

            print("\n============================================")
            print("✓ SCRAPING COMPLETE")
            print(f"  Total unique candidates processed: {len(self.candidates)}")
            print(f"  New candidates added: {len(new_candidates)}")
            print("============================================\n")

        except Exception as e:
            print(f"\n✗ A fatal error occurred: {e}")
            
            # Send error to n8n webhook
            if self.n8n_webhook_url:
                try:
                    error_payload = {
                        'timestamp': datetime.now().isoformat(),
                        'status': 'error',
                        'error_message': str(e)
                    }
                    requests.post(self.n8n_webhook_url, json=error_payload, timeout=10)
                except:
                    pass
                    
            import traceback
            traceback.print_exc()
        finally:
            await self.close()

async def main():
    scraper = JobTodayWebhookScraper()
    await scraper.run(headless=False)

if __name__ == "__main__":
    asyncio.run(main())