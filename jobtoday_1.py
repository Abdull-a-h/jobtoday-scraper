"""
JobToday Scraper - Complete Render Version
Includes chat history, chat summary, popup removal, and all advanced features
Optimized for containerized deployment on Render
"""
import asyncio
import json
import csv
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import os
from dotenv import load_dotenv
import requests
import logging
import traceback
import google.generativeai as genai

load_dotenv()
logger = logging.getLogger(__name__)

class JobTodayWebhookScraper:
    def __init__(self):
        self.email = os.getenv('JOBTODAY_EMAIL')
        self.password = os.getenv('JOBTODAY_PASSWORD')
        if not self.email or not self.password:
            raise ValueError("JOBTODAY_EMAIL and JOBTODAY_PASSWORD must be set")
            
        self.job_id = "p3j9ox" 
        self.base_url = "https://web.jobtoday.com"
        self.candidates = []
        self.processed_names = set()
        self.playwright = None
        self.browser = None
        self.job_role = None
        
        # Retry tracking for candidates
        self.candidate_retry_attempts = {}
        
        # Airtable setup
        self.airtable_token = os.getenv('AIRTABLE_PAT')
        self.airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
        self.airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME', 'Candidates')
        self.airtable_api_url = f"https://api.airtable.com/v0/{self.airtable_base_id}/{self.airtable_table_name}"
        
        # n8n webhook
        self.n8n_webhook_url = os.getenv('N8N_WEBHOOK_URL')
        
        # Google Gemini API setup
        self.gemini_api_key = os.getenv('GOOGLE_GEMINI')
        
        # Configuration logging
        if self.airtable_token and self.airtable_base_id:
            print("✓ Airtable configured")
            logger.info("Airtable configured")
        else:
            print("⚠ Airtable not fully configured")
            logger.warning("Airtable not fully configured")
            
        if self.n8n_webhook_url:
            print("✓ n8n webhook configured")
            logger.info("n8n webhook configured")
        else:
            print("⚠ n8n webhook not configured")
            logger.warning("n8n webhook not configured")
        
        if self.gemini_api_key:
            try:
                genai.configure(api_key=self.gemini_api_key)
                print("✓ Google Gemini API configured")
                logger.info("Google Gemini API configured")
            except Exception as e:
                print(f"✗ Failed to configure Google Gemini: {e}")
                logger.error(f"Failed to configure Google Gemini: {e}")
        else:
            print("⚠ Google Gemini not configured - chat summaries disabled")
            logger.warning("Google Gemini not configured")
        
    async def initialize_browser(self, headless=True):
        """Initialize browser with Render-compatible settings"""
        try:
            logger.info("Starting Playwright...")
            self.playwright = await async_playwright().start()
            
            logger.info("Launching Chromium browser...")
            
            # Browser args for containerized environments
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
            logger.info(f"Headless mode: {headless}")
            
            browsers_path = os.getenv('PLAYWRIGHT_BROWSERS_PATH', 'default')
            logger.info(f"PLAYWRIGHT_BROWSERS_PATH: {browsers_path}")
            
            try:
                self.browser = await self.playwright.chromium.launch(
                    headless=headless,
                    args=browser_args
                )
                logger.info("✓ Browser launched successfully")
            except Exception as launch_error:
                logger.error(f"Browser launch failed: {launch_error}")
                
                # Diagnostic checks
                logger.info("Attempting to locate Chromium executable...")
                try:
                    import subprocess
                    result = subprocess.run(['which', 'chromium'], capture_output=True, text=True)
                    logger.info(f"Chromium location check: {result.stdout}")
                    
                    result = subprocess.run(['playwright', 'install', '--dry-run', 'chromium'], 
                                          capture_output=True, text=True)
                    logger.info(f"Playwright status: {result.stdout}")
                except Exception as check_error:
                    logger.error(f"Diagnostic check failed: {check_error}")
                
                raise launch_error
            
            logger.info("Creating browser context...")
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            await self.context.grant_permissions(['geolocation'], origin=self.base_url)
            
            logger.info("Creating new page...")
            self.page = await self.context.new_page()
            
            logger.info("✓ Browser initialized successfully")
            print("✓ Browser initialized")
            return True
            
        except Exception as e:
            logger.error(f"✗ Browser initialization failed: {e}")
            logger.error(traceback.format_exc())
            print(f"✗ Browser initialization failed: {e}")
            raise
        
    async def login(self):
        try:
            print("→ Navigating to JobToday to log in...")
            logger.info("Navigating to login page")
            await self.page.goto(f"{self.base_url}/auth/login", wait_until='domcontentloaded', timeout=120000)
            await asyncio.sleep(3)
            
            post_login_selectors = [
                '[data-testid="tabs-my_jobs"]',
                'a[href="/jobs"]',
                'button:has-text("Post a job")',
                '[data-testid="sidebar"]'
            ]
            
            already_logged_in = False
            for selector in post_login_selectors:
                try:
                    element = await self.page.wait_for_selector(selector, timeout=10000, state='attached')
                    if element and await element.is_visible():
                        print("✓ Already logged in.")
                        logger.info("Already logged in")
                        already_logged_in = True
                        break
                except:
                    continue
            
            if already_logged_in:
                return True
                
            print("→ Not logged in. Proceeding with login...")
            logger.info("Not logged in, proceeding with login")
            
            try:
                await self.page.wait_for_selector('input[type="email"]', timeout=20000, state='visible')
            except:
                current_url = self.page.url
                if '/auth/login' not in current_url:
                    print("✓ Already logged in (redirected).")
                    logger.info("Already logged in (redirected)")
                    return True
                raise Exception("Login form not found")
            
            email_input = self.page.locator('input[type="email"]')
            password_input = self.page.locator('input[type="password"]')
            
            logger.info("Filling in credentials")
            await email_input.fill(self.email)
            await asyncio.sleep(1)
            await password_input.fill(self.password)
            await asyncio.sleep(1)
            
            print("→ Clicking submit button...")
            logger.info("Submitting login form")
            await self.page.click('button[type="submit"]')
            
            print("→ Waiting for login to complete...")
            logger.info("Waiting for login to complete")
            await asyncio.sleep(5)
            
            login_confirmed = False
            
            if '/auth/login' not in self.page.url:
                login_confirmed = True
                logger.info("Login confirmed - URL changed")
            
            if not login_confirmed:
                for selector in post_login_selectors:
                    try:
                        count = await self.page.locator(selector).count()
                        if count > 0:
                            login_confirmed = True
                            logger.info(f"Login confirmed - found element: {selector}")
                            break
                    except:
                        continue
            
            if login_confirmed:
                print("✓ Login successful")
                logger.info("✓ Login successful")
                await asyncio.sleep(3)
                return True
            else:
                print(f"✗ Could not confirm login")
                logger.error("Could not confirm login")
                raise Exception("Could not confirm successful login")
            
        except Exception as e:
            print(f"✗ Login failed: {e}")
            logger.error(f"Login failed: {e}")
            return False

    async def login_with_retry(self, max_attempts=3):
        for attempt in range(1, max_attempts + 1):
            print(f"\n→ Login attempt {attempt}/{max_attempts}")
            logger.info(f"Login attempt {attempt}/{max_attempts}")
            success = await self.login()
            if success:
                return True
            if attempt < max_attempts:
                logger.info("Waiting before retry...")
                await asyncio.sleep(10)
        return False
    
    async def save_session(self, filename='session.json'):
        try:
            storage = await self.context.storage_state()
            with open(filename, 'w') as f:
                json.dump(storage, f)
            print(f"✓ Session saved to {filename}")
            logger.info(f"Session saved to {filename}")
        except Exception as e:
            print(f"⚠ Could not save session: {e}")
            logger.warning(f"Could not save session: {e}")

    async def load_session(self, filename='session.json'):
        try:
            if not os.path.exists(filename):
                logger.info("No session file found")
                return False
            
            with open(filename, 'r') as f:
                storage_state = json.load(f)
            
            if hasattr(self, 'context'):
                await self.context.close()
            
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                storage_state=storage_state
            )
            
            await self.context.grant_permissions(['geolocation'], origin=self.base_url)
            self.page = await self.context.new_page()
            
            print(f"✓ Session loaded from {filename}")
            logger.info(f"Session loaded from {filename}")
            return True
        except Exception as e:
            print(f"⚠ Could not load session: {e}")
            logger.warning(f"Could not load session: {e}")
            return False
        
    async def scrape_job_role(self):
        try:
            print("\n→ Scraping job role...")
            logger.info("Scraping job role")
            main_job_url = f"{self.base_url}/jobs/{self.job_id}"
            await self.page.goto(main_job_url, wait_until='domcontentloaded', timeout=180000)
            await asyncio.sleep(5)
            
            role_selector = 'div.bg-white.rounded-b-xl div.text-black.font-bold.mb-1'
            await self.page.wait_for_selector(role_selector, timeout=40000)
            
            role_element = self.page.locator(role_selector).first
            self.job_role = await role_element.inner_text()
            
            print(f"   ✓ Job role scraped: {self.job_role}")
            logger.info(f"Job role scraped: {self.job_role}")
            return self.job_role
            
        except Exception as e:
            print(f"   ⚠ Could not scrape job role: {e}")
            logger.warning(f"Could not scrape job role: {e}")
            self.job_role = f"Job {self.job_id}"
            print(f"   → Using default role: {self.job_role}")
            logger.info(f"Using default role: {self.job_role}")
            return self.job_role

    async def dismiss_popups(self):
        """Dismiss any popups that might interfere with scraping"""
        try:
            logger.debug("Checking for popups to dismiss")
            popup_handlers = [
                {'selector': 'div:text-is("Not now")', 'action': 'click', 'name': '"Not now" button'},
                {'selector': 'button:has-text("Got it")', 'action': 'click', 'name': 'Contact Limit popup'},
                {'selector': 'button[aria-label="Close"]', 'action': 'click', 'name': 'Close button'},
                {'selector': '[role="dialog"]', 'action': 'escape', 'name': 'Dialog (Escape key)'},
                {'selector': '[id^=intercom-container], .intercom-lightweight-app, .intercom-messenger-frame', 
                 'action': 'remove', 'name': 'Intercom widget'}
            ]
            
            for handler in popup_handlers:
                try:
                    elements = self.page.locator(handler['selector'])
                    if await elements.count() > 0:
                        if handler['action'] == 'click':
                            for i in range(await elements.count()):
                                try:
                                    await elements.nth(i).click(timeout=5000, force=True)
                                    logger.info(f"Dismissed {handler['name']}")
                                    await asyncio.sleep(0.5)
                                except:
                                    pass
                        elif handler['action'] == 'escape':
                            try:
                                await self.page.keyboard.press('Escape')
                                logger.info(f"Pressed Escape for {handler['name']}")
                                await asyncio.sleep(0.5)
                            except:
                                pass
                        elif handler['action'] == 'remove':
                            await self.page.evaluate(f'document.querySelectorAll("{handler["selector"]}").forEach(el => el.remove())')
                            logger.info(f"Removed {handler['name']}")
                except:
                    pass
        except Exception as e:
            logger.debug(f"Error dismissing popups: {e}")

    async def scrape_chat_history(self):
        """Scrapes the full chat history from the messenger page"""
        print("      → Scraping chat history...")
        logger.info("Scraping chat history")
        chat_log = []

        try:
            chat_container_selector = 'div._container_i9fq9_12'
            await self.page.wait_for_selector(chat_container_selector, timeout=30000)
            
            all_blocks = self.page.locator(f'{chat_container_selector} > div')
            count = await all_blocks.count()
            logger.info(f"Found {count} chat blocks")

            for i in range(count):
                block = all_blocks.nth(i)
                entry = ""
                block_class = await block.get_attribute('class') or ""

                # Date separator
                if 'r-1awozwy' in block_class and 'r-5oul0u' in block_class:
                    date_locator = block.locator('div.r-1rbol0d')
                    if await date_locator.count() > 0:
                        date_text = await date_locator.first.inner_text(timeout=500)
                        if any(day in date_text for day in ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']):
                            entry = f"\n--- {date_text.strip()} ---"
                            if entry and entry not in chat_log:
                                chat_log.append(entry)
                            continue
                
                # Recruiter messages
                if 'r-88pszg' in block_class:
                    time_locator = block.locator('div[class*="r-a5pmau"], div[style*="margin-right: 12px;"]')
                    time = await time_locator.first.inner_text() if await time_locator.count() > 0 else ""
                    
                    msg_text_locator = block.locator('div.text-white.break-word')
                    if await msg_text_locator.count() > 0:
                        msg = await msg_text_locator.first.inner_text()
                        if msg:
                            entry = f"[{time}] Recruiter: {msg.strip()}"
                    elif await block.locator('div[class*="r-3hmvjm"]').count() > 0:
                        system_msg = await block.locator('div[class*="r-3hmvjm"]').first.inner_text()
                        entry = f"[{time}] System: {' '.join(system_msg.split())}"
                    
                    if entry and entry not in chat_log:
                        chat_log.append(entry)
                    continue

                # Candidate messages
                if 'r-1jkjb' in block_class:
                    if await block.locator('div[class*="r-6koalj"]').count() > 0:
                        continue

                    time_locator = block.locator('div[class*="r-1b7u577"]')
                    time = await time_locator.first.inner_text() if await time_locator.count() > 0 else ""
                    
                    msg_text_locator = block.locator('div.break-word[style*="white-space: pre-wrap;"]')
                    file_locator = block.locator('div[class*="r-1iln25a"]')
                    applied_locator = block.locator('div[class*="r-1gjx2kl"]')

                    if await msg_text_locator.count() > 0:
                        texts = await msg_text_locator.all_inner_texts()
                        msg = "\n".join(t.strip() for t in texts if t.strip())
                        if msg:
                            entry = f"[{time}] Candidate: {msg}"
                    elif await file_locator.count() > 0:
                        file_name = await file_locator.first.inner_text()
                        entry = f"[{time}] Candidate: [Sent File: {file_name.strip()}]"
                    elif await applied_locator.count() > 0:
                        system_msg = await applied_locator.first.inner_text()
                        entry = f"[{time}] System: {' '.join(system_msg.split())}"
                    
                    if entry and entry not in chat_log:
                        chat_log.append(entry)
                    continue

        except Exception as e:
            logger.error(f"Error scraping chat history: {e}")
            return "Error scraping chat history."

        if not chat_log:
            logger.info("No chat messages found")
            return "No chat messages found."

        final_log = "\n".join(chat_log).replace('\n\n\n', '\n\n')
        print(f"      ✓ Scraped {len(chat_log)} chat entries.")
        logger.info(f"Scraped {len(chat_log)} chat entries")
        return final_log

    async def generate_chat_summary(self, chat_history):
        """Generate a summary of the chat conversation using Google Gemini"""
        if not self.gemini_api_key:
            logger.info("Google Gemini not configured, skipping summary")
            return "N/A"
        
        if not chat_history or chat_history in ["N/A", "No chat messages found.", "Error scraping chat history."]:
            logger.info("No chat history to summarize")
            return "N/A"
            
        try:
            print("      → Generating conversation summary with Google Gemini...")
            logger.info("Generating chat summary")
            
            model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = (
                "You are a helpful assistant that summarizes job application conversations between a recruiter and a candidate. "
                "Provide a concise summary highlighting key points like candidate interest, availability, qualifications, questions asked, and next steps. "
                "Keep the summary under 150 words.\n\n"
                "Here is the conversation:\n\n"
                f"{chat_history}"
            )
            
            response = await asyncio.to_thread(model.generate_content, prompt)
            
            summary = response.text.strip()
            print(f"      ✓ Summary generated ({len(summary)} characters)")
            logger.info(f"Summary generated ({len(summary)} characters)")
            return summary
                
        except Exception as e:
            print(f"      ✗ Error generating chat summary: {e}")
            logger.error(f"Error generating chat summary: {e}")
            return "Error generating summary"

    async def scrape_section(self, section_name):
        """Scrape section with retry logic and chat integration"""
        section_url = f"{self.base_url}/jobs/{self.job_id}/{section_name}"
        print(f"\n→ Processing section: {section_url}")
        logger.info(f"Processing section: {section_name}")
        
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(section=section_name)
        
        # Load the section page
        page_loaded = False
        for attempt in range(1, 4):
            try:
                print(f"   → Loading page (attempt {attempt}/3)...")
                logger.info(f"Loading page attempt {attempt}/3")
                
                await self.page.goto(section_url, wait_until='networkidle', timeout=180000)
                logger.info("Page loaded with networkidle")
                await asyncio.sleep(3)
                
                current_url = self.page.url
                logger.info(f"Current URL: {current_url}")
                
                if '/auth/login' in current_url:
                    print("   ⚠ Session expired, re-authenticating...")
                    logger.warning("Session expired, re-authenticating")
                    if not await self.login_with_retry(max_attempts=2):
                        raise Exception("Re-login failed")
                    await self.page.goto(section_url, wait_until='networkidle', timeout=180000)
                    await asyncio.sleep(3)
                
                list_container_selector = 'div.col-span-1.overflow-y-auto'
                logger.info(f"Waiting for list container")
                
                try:
                    await self.page.wait_for_selector(list_container_selector, timeout=60000)
                    container_count = await self.page.locator(list_container_selector).count()
                    logger.info(f"✓ Found {container_count} list containers")
                    print("   ✓ List container found")
                    page_loaded = True
                    break
                except PlaywrightTimeout as timeout_error:
                    logger.error(f"List container not found: {timeout_error}")
                    
                    if attempt < 3:
                        logger.info("Retrying page load...")
                        await asyncio.sleep(5)
                    else:
                        raise
                    
            except Exception as e:
                logger.error(f"Load attempt {attempt} failed: {e}")
                print(f"   ⚠ Load attempt {attempt} failed: {e}")
                if attempt < 3:
                    await asyncio.sleep(5)
                else:
                    raise
        
        if not page_loaded:
            raise Exception(f"Could not load section {section_name}")
        
        await asyncio.sleep(3)
        
        # Count candidates
        candidate_button_selector = 'button:has(img[alt$="\'s avatar"])'
        logger.info(f"Looking for candidates")
        
        try:
            await self.page.wait_for_selector(candidate_button_selector, timeout=30000)
            total_candidates = await self.page.locator(candidate_button_selector).count()
            logger.info(f"Found {total_candidates} candidate buttons")
        except PlaywrightTimeout:
            logger.warning(f"No candidates found in '{section_name}' section")
            print(f"   ! No candidates found in '{section_name}' section")
            return
        
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(total=total_candidates)

        print(f"   ✓ Found {total_candidates} candidates")
        logger.info(f"Starting to process {total_candidates} candidates")

        # Process each candidate with retry logic
        i = 0
        while i < total_candidates:
            candidate_name = f"Candidate {i+1}"
            try:
                logger.info(f"{'='*50}")
                logger.info(f"Processing candidate {i+1}/{total_candidates}")
                print(f"\n--- Processing candidate {i+1}/{total_candidates} ---")
                
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(
                        candidate=f"Loading {i+1}",
                        processed=i
                    )
                
                # Navigate back to list if needed
                if i > 0:
                    logger.info("Navigating back to list view...")
                    
                    try:
                        await self.page.go_back(wait_until='domcontentloaded', timeout=60000)
                        logger.info("Used browser back button")
                        await asyncio.sleep(2)
                    except:
                        logger.info("Back button failed, using direct navigation")
                        await self.page.goto(section_url, wait_until='domcontentloaded', timeout=120000)
                        await asyncio.sleep(3)
                    
                    try:
                        await self.page.wait_for_selector(candidate_button_selector, timeout=40000)
                        logger.info("List reloaded successfully")
                    except:
                        logger.error("Could not reload list, trying full page reload")
                        await self.page.goto(section_url, wait_until='networkidle', timeout=180000)
                        await asyncio.sleep(3)
                        await self.page.wait_for_selector(candidate_button_selector, timeout=40000)
                
                # Verify candidate count
                current_count = await self.page.locator(candidate_button_selector).count()
                logger.info(f"Current candidate count: {current_count}")
                
                if i >= current_count:
                    logger.warning(f"Candidate index {i} out of range (only {current_count} candidates)")
                    print(f"   ⚠ Skipping - candidate index out of range")
                    i += 1
                    continue
                
                # Get candidate button and name
                candidate_button = self.page.locator(candidate_button_selector).nth(i)
                logger.info(f"Got candidate button at index {i}")
                
                try:
                    candidate_name_elem = candidate_button.locator('.font-bold').first
                    candidate_name = await candidate_name_elem.inner_text(timeout=20000)
                    logger.info(f"Candidate name: {candidate_name}")
                    print(f"   → Candidate name: {candidate_name}")
                except Exception as name_error:
                    logger.error(f"Could not get candidate name: {name_error}")
                    candidate_name = f"Candidate {i+1}"
                
                # Skip if already processed
                if candidate_name in self.processed_names:
                    logger.info(f"Skipping {candidate_name} - already processed")
                    print(f"   ✓ Skipping (already processed)")
                    i += 1
                    continue
                
                # Retry logic
                max_retries = 1
                attempt_count = self.candidate_retry_attempts.get(candidate_name, 0)
                
                if attempt_count > max_retries:
                    logger.warning(f"Max retries reached for {candidate_name}")
                    print(f"   ! Max retries reached. Skipping.")
                    self.processed_names.add(candidate_name)
                    i += 1
                    continue
                
                self.candidate_retry_attempts[candidate_name] = attempt_count + 1
                
                if attempt_count > 0:
                    logger.info(f"RETRY attempt {attempt_count + 1} for {candidate_name}")
                    print(f"   → RETRY attempt {attempt_count + 1}")
                
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(candidate=candidate_name)
                
                # Get application date
                application_date = "N/A"
                try:
                    date_locator = candidate_button.locator('p:has-text("Applied on")')
                    if await date_locator.count() > 0:
                        application_date = await date_locator.inner_text(timeout=10000)
                        logger.info(f"Application date: {application_date}")
                except Exception as date_error:
                    logger.warning(f"Could not get application date: {date_error}")
                
                # Click candidate
                logger.info("Clicking candidate button...")
                print(f"   → Clicking candidate...")
                
                try:
                    await candidate_button.click(timeout=60000)
                    logger.info("Click successful")
                except Exception as click_error:
                    logger.error(f"Click failed: {click_error}")
                    print(f"   ✗ Click failed")
                    i += 1
                    continue
                
                await asyncio.sleep(3)
                
                # Wait for profile
                profile_selector = 'button:has-text("Chat with")'
                logger.info(f"Waiting for profile to load")
                
                try:
                    await self.page.wait_for_selector(profile_selector, timeout=60000)
                    logger.info("✓ Profile loaded")
                    print("   ✓ Profile loaded")
                except PlaywrightTimeout:
                    logger.error("Profile load timeout")
                    print(f"   ✗ Profile timeout, skipping")
                    i += 1
                    continue
                
                # Dismiss popups
                await self.dismiss_popups()
                await asyncio.sleep(2)
                
                # Scrape details
                logger.info("Scraping candidate details...")
                print(f"   → Scraping details...")
                
                try:
                    details = await self.scrape_candidate_details(self.page.url, application_date)
                    
                    # Validate phone number
                    phone = details.get('phone', 'N/A')
                    is_phone_valid = phone and phone != "N/A" and '…' not in phone and len(phone) >= 9
                    
                    if is_phone_valid:
                        logger.info(f"✓ Successfully scraped: {details.get('name', 'N/A')}")
                        print(f"   ✓ Scraped: {details.get('name', 'N/A')}")
                        self.candidates.append(details)
                        self.processed_names.add(candidate_name)
                        
                        if hasattr(self, 'progress_tracker'):
                            self.progress_tracker.update(processed=i+1)
                        
                        i += 1  # SUCCESS - move to next candidate
                    else:
                        logger.warning(f"Phone scraping failed for {candidate_name}")
                        print(f"   ✗ Phone scraping failed. Will retry this candidate.")
                        # Don't increment i - will retry same candidate
                            
                except Exception as scrape_error:
                    logger.error(f"Error scraping details: {scrape_error}")
                    logger.error(traceback.format_exc())
                    print(f"   ✗ Error scraping: {scrape_error}")
                    i += 1  # Move on after error
                
            except Exception as e:
                logger.error(f"Error processing candidate {i+1}: {e}")
                logger.error(traceback.format_exc())
                print(f"   ✗ Error processing candidate {i+1}: {e}")
                
                try:
                    logger.info("Attempting to recover by navigating back to list...")
                    await self.page.goto(section_url, wait_until='domcontentloaded', timeout=120000)
                    await asyncio.sleep(3)
                except:
                    logger.error("Could not recover")
                
                i += 1  # Move on after critical error
                continue
            
            finally:
                if self.page.is_closed():
                    logger.error("Page closed unexpectedly")
                    print("   ! Browser closed unexpectedly. Halting.")
                    break
                if i < total_candidates:
                    logger.info("Resetting to list view...")
                    await self.page.goto(section_url, wait_until="domcontentloaded", timeout=90000)
                    await asyncio.sleep(3)
        
        logger.info(f"Finished section: {section_name}")
        print(f"\n✓ Finished section: {section_name}")
        print(f"   Total: {total_candidates}, Scraped: {len([c for c in self.candidates if c.get('name') != 'N/A'])}")

    async def scrape_candidate_details(self, candidate_url, application_date):
        """Scrape candidate details including chat history and summary"""
        details = {
            'profile_url': candidate_url, 
            'application_date': application_date, 
            'job_role': self.job_role
        }
        
        try:
            profile_pane = self.page.locator('div.col-span-1.overflow-y-auto:has(button:has-text("Chat with"))')

            async def get_text(locator, timeout=10000):
                try: 
                    await locator.wait_for(timeout=timeout, state='attached')
                    text = await locator.inner_text(timeout=timeout)
                    return text if text else "N/A"
                except:
                    return "N/A"

            await self.dismiss_popups()
            await asyncio.sleep(1)
            
            # Name
            details['name'] = await get_text(profile_pane.locator('div.font-bold.text-2xl').first)
            logger.info(f"Name: {details['name']}")
            
            # Phone with retry logic
            try:
                phone_container = profile_pane.locator('div.flex.items-center.gap-2.mt-2:has(img[src*="IconPhoneFilled28"])')
                phone_number = "N/A"
                max_attempts = 3

                for attempt in range(1, max_attempts + 1):
                    phone_span = phone_container.locator('span').first
                    phone_text = await phone_span.text_content(timeout=6000) or ""
                    current_phone_value = phone_text.replace("Show phone", "").strip()

                    if current_phone_value and '…' not in current_phone_value and len(current_phone_value) >= 9:
                        phone_number = current_phone_value
                        logger.info(f"Phone retrieved successfully: {phone_number}")
                        break

                    show_phone_button = profile_pane.locator('span.cursor-pointer.text-jt-blue-500:has-text("Show phone")')
                    if await show_phone_button.count() > 0:
                        try:
                            logger.info(f"Clicking 'Show phone' button (attempt {attempt})")
                            await show_phone_button.click(timeout=5000, force=True)
                            await asyncio.sleep(2)
                            await self.dismiss_popups()
                            await asyncio.sleep(1)

                            phone_text = await phone_span.text_content(timeout=6000) or ""
                            phone_number = phone_text.replace("Show phone", "").strip()
                            
                            if phone_number and '…' not in phone_number and len(phone_number) >= 9:
                                logger.info(f"Phone retrieved after click: {phone_number}")
                                break
                        except Exception as click_error:
                            logger.warning(f"Error clicking show phone: {click_error}")
                            pass
                    
                    if attempt < max_attempts:
                        logger.info(f"Phone not fully loaded, waiting...")
                        await asyncio.sleep(2)
                    else:
                        phone_text = await phone_span.text_content(timeout=6000) or ""
                        phone_number = phone_text.replace("Show phone", "").strip()
                        logger.warning(f"Final phone value: {phone_number}")
                
                details['phone'] = phone_number

            except Exception as e: 
                logger.error(f"Error getting phone: {e}")
                details['phone'] = "N/A"
            
            await self.dismiss_popups()
            
            # Email
            details['email'] = await get_text(profile_pane.locator('a[href^="mailto:"]').first)
            logger.info(f"Email: {details['email']}")
            
            # Location
            details['location'] = await get_text(profile_pane.locator('div:has(img[src*="IconPinThinBlack20"]) > span').first)
            logger.info(f"Location: {details['location']}")
            
            # About
            try:
                about_div = profile_pane.locator('hr.my-6 + div.px-4.break-word').first
                details['about'] = await get_text(about_div)
            except:
                details['about'] = "N/A"
            
            # Certificates
            try:
                certs_header = profile_pane.locator('div.font-bold.text-xl:has-text("Certificates")').first
                certs_block = certs_header.locator('xpath=./following-sibling::div[1]')
                details['certificates'] = await get_text(certs_block)
            except:
                details['certificates'] = "N/A"
                
            # Experience
            try:
                exp_header = profile_pane.locator('div.font-bold.text-xl:has-text("Experience")').first
                exp_block = exp_header.locator('xpath=./following-sibling::div[1]')
                details['experience'] = await get_text(exp_block)
            except:
                details['experience'] = "N/A"
                
            # Languages
            try:
                lang_header = profile_pane.locator('div.font-bold.text-xl:has-text("Languages")').first
                lang_block = lang_header.locator('xpath=./following-sibling::div[1]')
                details['languages'] = await get_text(lang_block)
            except:
                details['languages'] = "N/A"

            # Chat history and summary
            details['chat_history'] = 'N/A'
            details['chat_summary'] = 'N/A'
            
            try:
                print("   → Attempting to scrape chat history...")
                logger.info("Attempting to scrape chat history")
                
                await self.dismiss_popups()
                
                chat_button_selector = 'button:has-text("Chat with")'
                chat_button = profile_pane.locator(chat_button_selector).first

                if await chat_button.count() > 0:
                    logger.info("Clicking chat button")
                    await chat_button.click()
                    
                    print("      → Navigating to messenger...")
                    logger.info("Navigating to messenger")
                    await self.page.wait_for_url(lambda url: '/messenger' in url, timeout=90000)
                    await asyncio.sleep(3)
                    
                    await self.dismiss_popups()
                    
                    details['chat_history'] = await self.scrape_chat_history()
                    
                    # Generate summary
                    details['chat_summary'] = await self.generate_chat_summary(details['chat_history'])
                    
                    logger.info("Chat history and summary completed")
                else:
                    logger.info("Chat button not found")
                    print("   ! Chat button not found.")

            except Exception as e:
                logger.error(f"Could not scrape chat history: {e}")
                print(f"   ✗ Could not scrape chat history: {str(e).splitlines()[0]}")
                if '/messenger' in self.page.url:
                    try:
                        await self.page.go_back(wait_until='domcontentloaded')
                        logger.info("Navigated back from messenger")
                    except Exception as nav_e:
                        logger.error(f"Failed to navigate back: {nav_e}")

            return details
            
        except Exception as e:
            logger.error(f"Error in scrape_candidate_details: {e}")
            logger.error(traceback.format_exc())
            print(f"      ✗ Error in scrape_candidate_details: {e}")
            return details

    def get_existing_profiles(self):
        """Fetch existing profiles from Airtable"""
        if not self.airtable_token or not self.airtable_base_id:
            return set()
            
        try:
            print("→ Checking Airtable for existing candidates...")
            logger.info("Checking Airtable for existing candidates")
            headers = {'Authorization': f'Bearer {self.airtable_token}'}
            
            existing_urls = set()
            offset = None
            
            while True:
                params = {'fields[]': 'Profile URL', 'pageSize': 100}
                if offset:
                    params['offset'] = offset
                
                response = requests.get(self.airtable_api_url, headers=headers, params=params, timeout=60)
                
                if response.status_code != 200:
                    logger.warning(f"Could not fetch records: {response.status_code}")
                    print(f"   ⚠ Could not fetch records: {response.status_code}")
                    return set()
                
                data = response.json()
                for record in data.get('records', []):
                    url = record.get('fields', {}).get('Profile URL', '')
                    if url:
                        existing_urls.add(url)
                
                offset = data.get('offset')
                if not offset:
                    break
            
            print(f"   ✓ Found {len(existing_urls)} existing candidates")
            logger.info(f"Found {len(existing_urls)} existing candidates in Airtable")
            return existing_urls
            
        except Exception as e:
            logger.error(f"Error fetching from Airtable: {e}")
            print(f"   ✗ Error: {e}")
            return set()

    def push_to_airtable(self):
        """Push new candidates to Airtable"""
        if not self.airtable_token or not self.airtable_base_id:
            print("⚠ Airtable not configured")
            logger.warning("Airtable not configured")
            return []
            
        try:
            print("\n→ Pushing to Airtable...")
            logger.info("Pushing to Airtable")
            
            existing_urls = self.get_existing_profiles()
            new_candidates = [c for c in self.candidates if c.get('profile_url') not in existing_urls]
            
            if not new_candidates:
                print("   ! All candidates already exist")
                logger.info("All candidates already exist in Airtable")
                return []
            
            print(f"   → Pushing {len(new_candidates)} new candidates")
            logger.info(f"Pushing {len(new_candidates)} new candidates")
            
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
                        'Chat History': candidate.get('chat_history', ''),
                        'Chat Summary': candidate.get('chat_summary', ''),
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
                
                response = requests.post(self.airtable_api_url, json=payload, headers=headers, timeout=60)
                
                if response.status_code == 200:
                    total_pushed += len(batch)
                    logger.info(f"Pushed batch {i//batch_size + 1}")
                    print(f"   ✓ Pushed batch {i//batch_size + 1}")
                else:
                    logger.error(f"Error pushing batch: {response.status_code}")
                    print(f"   ✗ Error: {response.status_code}")
            
            print(f"✓ Pushed {total_pushed} candidates")
            logger.info(f"✓ Pushed {total_pushed} candidates to Airtable")
            return new_candidates
            
        except Exception as e:
            logger.error(f"Airtable error: {e}")
            logger.error(traceback.format_exc())
            print(f"✗ Airtable error: {e}")
            return []

    def send_to_n8n_webhook(self, new_candidates):
        """Send to n8n webhook"""
        if not self.n8n_webhook_url:
            return
            
        try:
            print("\n→ Sending to n8n...")
            logger.info("Sending to n8n webhook")
            
            payload = {
                'timestamp': datetime.now().isoformat(),
                'job_id': self.job_id,
                'total_scraped': len(self.candidates),
                'new_candidates_count': len(new_candidates),
                'new_candidates': new_candidates,
                'status': 'success'
            }
            
            response = requests.post(self.n8n_webhook_url, json=payload, timeout=60)
            
            if response.status_code in [200, 201, 204]:
                print(f"✓ Sent {len(new_candidates)} to n8n")
                logger.info(f"✓ Sent {len(new_candidates)} candidates to n8n")
            else:
                print(f"⚠ Webhook status {response.status_code}")
                logger.warning(f"Webhook returned status {response.status_code}")
                
        except Exception as e:
            logger.error(f"n8n webhook error: {e}")
            print(f"✗ n8n error: {e}")

    async def save_to_json(self, filename='candidates_detailed.json'):
        output = {
            'scraped_at': datetime.now().isoformat(),
            'job_id': self.job_id,
            'total_candidates': len(self.candidates),
            'candidates': self.candidates
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        print(f"✓ Saved to {filename}")
        logger.info(f"Saved to {filename}")

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
            print(f"✓ Exported to {filename}")
            logger.info(f"Exported to {filename}")
        except Exception as e:
            logger.error(f"CSV export error: {e}")
            print(f"✗ CSV error: {e}")

    async def close(self):
        if self.browser:
            await self.browser.close()
            logger.info("Browser closed")
        if self.playwright:
            await self.playwright.stop()
            logger.info("Playwright stopped")
        print("✓ Browser closed")

    async def run(self, headless=True):
        try:
            await self.initialize_browser(headless=headless)
            
            # Load or create session
            session_loaded = await self.load_session()
            
            if not session_loaded:
                if not await self.login_with_retry(max_attempts=3):
                    print("✗ Login failed")
                    logger.error("Login failed after retries")
                    return
                await self.save_session()
            else:
                print("→ Verifying session...")
                logger.info("Verifying existing session...")
                await self.page.goto(self.base_url, wait_until='domcontentloaded', timeout=120000)
                await asyncio.sleep(3)
                
                if '/auth/login' in self.page.url:
                    print("   Session expired, re-login...")
                    logger.warning("Session expired, re-authenticating")
                    if not await self.login_with_retry(max_attempts=3):
                        print("✗ Re-login failed")
                        logger.error("Re-login failed")
                        return
                    await self.save_session()
                else:
                    print("   ✓ Session valid")
                    logger.info("Session still valid")

            # Scrape job role
            await self.scrape_job_role()

            logger.info("Ready to start scraping sections...")
            print("\n→ Starting to scrape sections...")

            # Scrape sections
            sections = ['recommended', 'incoming']
            for section in sections:
                if self.page.is_closed():
                    logger.error("Page closed unexpectedly")
                    break
                
                logger.info(f"About to scrape section: {section}")
                await self.scrape_section(section)

            if not self.candidates:
                print("\n✗ No candidates scraped")
                logger.warning("No candidates were scraped")
                return

            # Save results
            logger.info("Saving results...")
            await self.save_to_json()
            await self.export_to_csv()
            
            # Push to Airtable
            logger.info("Pushing to Airtable...")
            new_candidates = self.push_to_airtable()
            
            # Send to n8n
            logger.info("Sending to n8n webhook...")
            self.send_to_n8n_webhook(new_candidates)

            print("\n" + "="*50)
            print("✓ SCRAPING COMPLETE")
            print(f"  Total: {len(self.candidates)}")
            print(f"  New: {len(new_candidates)}")
            print("="*50)
            
            logger.info(f"✓ Scraping complete - Total: {len(self.candidates)}, New: {len(new_candidates)}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Fatal error in run(): {error_msg}")
            logger.error(traceback.format_exc())
            print(f"\n✗ Fatal error: {e}")
            
            # Send error to n8n
            if self.n8n_webhook_url:
                try:
                    error_payload = {
                        'timestamp': datetime.now().isoformat(),
                        'status': 'error',
                        'error_message': error_msg
                    }
                    requests.post(self.n8n_webhook_url, json=error_payload, timeout=20)
                    logger.info("Error notification sent to n8n")
                except Exception as webhook_error:
                    logger.error(f"Could not send error to n8n: {webhook_error}")
                        
        finally:
            await self.close()

async def main():
    scraper = JobTodayWebhookScraper()
    await scraper.run(headless=False)

if __name__ == "__main__":
    asyncio.run(main())