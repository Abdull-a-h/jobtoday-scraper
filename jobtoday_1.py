"""
JobToday Scraper - Robust Version
Fixes navigation issues and improves state management
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
        
        # Airtable setup
        self.airtable_token = os.getenv('AIRTABLE_PAT')
        self.airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
        self.airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME', 'Candidates')
        self.airtable_api_url = f"https://api.airtable.com/v0/{self.airtable_base_id}/{self.airtable_table_name}"
        
        # n8n webhook
        self.n8n_webhook_url = os.getenv('N8N_WEBHOOK_URL')
        
        if self.airtable_token and self.airtable_base_id:
            print("✓ Airtable configured")
        if self.n8n_webhook_url:
            print("✓ n8n webhook configured")
        
    async def initialize_browser(self, headless=True):
        """Initialize browser with Render-compatible settings"""
        try:
            logger.info("Starting Playwright...")
            self.playwright = await async_playwright().start()
            
            logger.info("Launching Chromium browser...")
            
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
            
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                args=browser_args
            )
            
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            await self.context.grant_permissions(['geolocation'], origin=self.base_url)
            self.page = await self.context.new_page()
            
            logger.info("✓ Browser initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Browser initialization failed: {e}")
            raise
        
    async def login(self):
        try:
            print("→ Navigating to JobToday to log in...")
            await self.page.goto(f"{self.base_url}/auth/login", wait_until='domcontentloaded', timeout=60000)
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
                    element = await self.page.wait_for_selector(selector, timeout=5000, state='attached')
                    if element and await element.is_visible():
                        print("✓ Already logged in.")
                        already_logged_in = True
                        break
                except:
                    continue
            
            if already_logged_in:
                return True
                
            print("→ Not logged in. Proceeding with login...")
            
            try:
                await self.page.wait_for_selector('input[type="email"]', timeout=10000, state='visible')
            except:
                current_url = self.page.url
                if '/auth/login' not in current_url:
                    print("✓ Already logged in (redirected).")
                    return True
                raise Exception("Login form not found")
            
            email_input = self.page.locator('input[type="email"]')
            password_input = self.page.locator('input[type="password"]')
            
            await email_input.fill(self.email)
            await asyncio.sleep(1)
            await password_input.fill(self.password)
            await asyncio.sleep(1)
            
            print("→ Clicking submit button...")
            await self.page.click('button[type="submit"]')
            
            print("→ Waiting for login to complete...")
            await asyncio.sleep(5)
            
            login_confirmed = False
            
            if '/auth/login' not in self.page.url:
                login_confirmed = True
            
            if not login_confirmed:
                for selector in post_login_selectors:
                    try:
                        count = await self.page.locator(selector).count()
                        if count > 0:
                            login_confirmed = True
                            break
                    except:
                        continue
            
            if login_confirmed:
                print("✓ Login successful")
                await asyncio.sleep(3)
                return True
            else:
                print(f"✗ Could not confirm login")
                raise Exception("Could not confirm successful login")
            
        except Exception as e:
            print(f"✗ Login failed: {e}")
            return False

    async def login_with_retry(self, max_attempts=3):
        for attempt in range(1, max_attempts + 1):
            print(f"\n→ Login attempt {attempt}/{max_attempts}")
            success = await self.login()
            if success:
                return True
            if attempt < max_attempts:
                await asyncio.sleep(10)
        return False
    
    async def save_session(self, filename='session.json'):
        try:
            storage = await self.context.storage_state()
            with open(filename, 'w') as f:
                json.dump(storage, f)
            print(f"✓ Session saved to {filename}")
        except Exception as e:
            print(f"⚠ Could not save session: {e}")

    async def load_session(self, filename='session.json'):
        try:
            if not os.path.exists(filename):
                return False
            
            with open(filename, 'r') as f:
                storage_state = json.load(f)
            
            # Close existing context and create new one with storage state
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
            return True
        except Exception as e:
            print(f"⚠ Could not load session: {e}")
            return False
        
    async def scrape_job_role(self):
        try:
            print("\n→ Scraping job role...")
            main_job_url = f"{self.base_url}/jobs/{self.job_id}"
            await self.page.goto(main_job_url, wait_until='domcontentloaded', timeout=90000)
            await asyncio.sleep(5)
            
            role_selector = 'div.bg-white.rounded-b-xl div.text-black.font-bold.mb-1'
            await self.page.wait_for_selector(role_selector, timeout=20000)
            
            role_element = self.page.locator(role_selector).first
            self.job_role = await role_element.inner_text()
            
            print(f"   ✓ Job role scraped: {self.job_role}")
            return self.job_role
            
        except Exception as e:
            print(f"   ⚠ Could not scrape job role: {e}")
            self.job_role = f"Job {self.job_id}"
            print(f"   → Using default role: {self.job_role}")
            return self.job_role

    async def wait_for_stable_page(self, timeout=10000):
        """Wait for page to stop navigating/loading"""
        try:
            # Wait a bit for any pending navigations
            await asyncio.sleep(2)
            
            # Check if page is still loading
            for _ in range(5):
                try:
                    await self.page.wait_for_load_state('networkidle', timeout=5000)
                    break
                except:
                    await asyncio.sleep(1)
            
            return True
        except:
            return False

    async def scrape_section(self, section_name):
        """Improved section scraping with better state management"""
        section_url = f"{self.base_url}/jobs/{self.job_id}/{section_name}"
        print(f"\n→ Processing section: {section_url}")
        
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(section=section_name)
        
        # Load the section page with retries
        page_loaded = False
        for attempt in range(1, 4):
            try:
                print(f"   → Loading page (attempt {attempt}/3)...")
                
                # Use goto with wait_until='load' instead of 'domcontentloaded'
                await self.page.goto(section_url, wait_until='load', timeout=60000)
                await self.wait_for_stable_page()
                
                # Check for login redirect
                if '/auth/login' in self.page.url:
                    print("   ⚠ Session expired, re-authenticating...")
                    if not await self.login_with_retry(max_attempts=2):
                        raise Exception("Re-login failed")
                    await self.page.goto(section_url, wait_until='load', timeout=60000)
                    await self.wait_for_stable_page()
                
                # Wait for list container
                list_container_selector = 'div.col-span-1.overflow-y-auto'
                await self.page.wait_for_selector(list_container_selector, timeout=30000)
                print("   ✓ List container found")
                page_loaded = True
                break
                
            except Exception as e:
                print(f"   ⚠ Load attempt {attempt} failed: {e}")
                if attempt < 3:
                    await asyncio.sleep(5)
                else:
                    raise
        
        if not page_loaded:
            raise Exception(f"Could not load section {section_name}")
        
        await asyncio.sleep(5)
        
        # Count candidates
        candidate_button_selector = 'button:has(img[alt$="\'s avatar"])'
        
        try:
            # Wait for candidates to appear
            await self.page.wait_for_selector(candidate_button_selector, timeout=15000)
            total_candidates = await self.page.locator(candidate_button_selector).count()
        except:
            print(f"   ! No candidates found in '{section_name}' section")
            return
        
        if hasattr(self, 'progress_tracker'):
            self.progress_tracker.update(total=total_candidates)

        print(f"   ✓ Found {total_candidates} candidates")

        # Process each candidate
        for i in range(total_candidates):
            try:
                print(f"\n--- Starting candidate {i+1}/{total_candidates} ---")
                
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(
                        candidate=f"Loading {i+1}",
                        processed=i
                    )
                
                # CRITICAL: Always reload the list page before accessing candidate
                # This ensures we have fresh button references
                await self.page.goto(section_url, wait_until='load', timeout=60000)
                await self.wait_for_stable_page()
                await asyncio.sleep(3)
                
                # Wait for candidates to load
                await self.page.wait_for_selector(candidate_button_selector, timeout=30000)
                
                # Get the specific candidate button
                candidate_button = self.page.locator(candidate_button_selector).nth(i)
                
                # Get candidate name
                try:
                    candidate_name = await candidate_button.locator('.font-bold').first.inner_text(timeout=10000)
                    print(f"   → Candidate name: {candidate_name}")
                except:
                    candidate_name = f"Candidate {i+1}"
                
                # Skip if already processed
                if candidate_name in self.processed_names:
                    print(f"   ✓ Skipping (already processed)")
                    continue
                
                if hasattr(self, 'progress_tracker'):
                    self.progress_tracker.update(candidate=candidate_name)
                
                # Get application date before clicking
                application_date = "N/A"
                try:
                    date_locator = candidate_button.locator('p:has-text("Applied on")')
                    if await date_locator.count() > 0:
                        application_date = await date_locator.inner_text(timeout=5000)
                except:
                    pass
                
                # Click candidate
                print(f"   → Clicking candidate...")
                await candidate_button.click(timeout=30000)
                await asyncio.sleep(3)
                
                # Wait for profile to load
                profile_selector = 'button:has-text("Chat with")'
                
                try:
                    await self.page.wait_for_selector(profile_selector, timeout=30000)
                    print("   ✓ Profile loaded")
                except PlaywrightTimeout:
                    print(f"   ✗ Profile timeout, skipping")
                    continue
                
                # Remove popups
                try:
                    await self.page.evaluate('document.querySelectorAll("[id^=intercom-container], .intercom-lightweight-app").forEach(el => el.remove())')
                except:
                    pass
                
                await asyncio.sleep(2)
                
                # Scrape details
                print(f"   → Scraping details...")
                try:
                    details = await self.scrape_candidate_details(self.page.url, application_date)
                    self.candidates.append(details)
                    self.processed_names.add(candidate_name)
                    print(f"   ✓ Scraped: {details.get('name', 'N/A')}")
                    
                    if hasattr(self, 'progress_tracker'):
                        self.progress_tracker.update(processed=i+1)
                        
                except Exception as e:
                    print(f"   ✗ Error scraping: {e}")
                
                # Don't navigate back here - we'll reload at the start of next iteration
                
            except Exception as e:
                print(f"   ✗ Error processing candidate {i+1}: {e}")
                import traceback
                print(f"   Traceback: {traceback.format_exc()[:500]}")
                # Continue to next candidate
                continue
        
        print(f"\n✓ Finished section: {section_name}")
        print(f"   Total: {total_candidates}, Scraped: {len([c for c in self.candidates if c.get('name') != 'N/A'])}")

    async def scrape_candidate_details(self, candidate_url, application_date):
        """Scrape candidate details from profile"""
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
                except:
                    return "N/A"

            # Name
            details['name'] = await get_text(profile_pane.locator('div.font-bold.text-2xl').first)
            
            # Phone
            try:
                phone_container = profile_pane.locator('div.flex.items-center.gap-2.mt-2:has(img[src*="IconPhoneFilled28"])')
                show_button = profile_pane.locator('span.cursor-pointer.text-jt-blue-500:has-text("Show phone")')
                
                if await show_button.count() > 0:
                    try:
                        await show_button.click(timeout=5000)
                        await asyncio.sleep(2)
                    except:
                        pass
                    
                    phone_span = phone_container.locator('span').first
                    phone_text = await phone_span.text_content(timeout=3000)
                    phone_number = phone_text.strip() if phone_text else "N/A"
                    
                    if "Show phone" in phone_number:
                        phone_number = phone_number.replace("Show phone", "").strip()
                else:
                    phone_span = phone_container.locator('span').first
                    phone_text = await phone_span.text_content(timeout=3000)
                    phone_number = phone_text.strip() if phone_text else "N/A"
                
                details['phone'] = phone_number
            except: 
                details['phone'] = "N/A"
            
            # Email
            details['email'] = await get_text(profile_pane.locator('a[href^="mailto:"]').first)
            
            # Location
            details['location'] = await get_text(profile_pane.locator('div:has(img[src*="IconPinThinBlack20"]) > span').first)
            
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

            return details
            
        except Exception as e:
            print(f"      ✗ Error in scrape_candidate_details: {e}")
            return details

    def get_existing_profiles(self):
        """Fetch existing profiles from Airtable"""
        if not self.airtable_token or not self.airtable_base_id:
            return set()
            
        try:
            print("→ Checking Airtable for existing candidates...")
            headers = {'Authorization': f'Bearer {self.airtable_token}'}
            
            existing_urls = set()
            offset = None
            
            while True:
                params = {'fields[]': 'Profile URL', 'pageSize': 100}
                if offset:
                    params['offset'] = offset
                
                response = requests.get(self.airtable_api_url, headers=headers, params=params, timeout=30)
                
                if response.status_code != 200:
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
            return existing_urls
            
        except Exception as e:
            print(f"   ✗ Error: {e}")
            return set()

    def push_to_airtable(self):
        """Push new candidates to Airtable"""
        if not self.airtable_token or not self.airtable_base_id:
            print("⚠ Airtable not configured")
            return []
            
        try:
            print("\n→ Pushing to Airtable...")
            
            existing_urls = self.get_existing_profiles()
            new_candidates = [c for c in self.candidates if c.get('profile_url') not in existing_urls]
            
            if not new_candidates:
                print("   ! All candidates already exist")
                return []
            
            print(f"   → Pushing {len(new_candidates)} new candidates")
            
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
                
                response = requests.post(self.airtable_api_url, json=payload, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    total_pushed += len(batch)
                    print(f"   ✓ Pushed batch {i//batch_size + 1}")
                else:
                    print(f"   ✗ Error: {response.status_code}")
            
            print(f"✓ Pushed {total_pushed} candidates")
            return new_candidates
            
        except Exception as e:
            print(f"✗ Airtable error: {e}")
            return []

    def send_to_n8n_webhook(self, new_candidates):
        """Send to n8n webhook"""
        if not self.n8n_webhook_url:
            return
            
        try:
            print("\n→ Sending to n8n...")
            
            payload = {
                'timestamp': datetime.now().isoformat(),
                'job_id': self.job_id,
                'total_scraped': len(self.candidates),
                'new_candidates_count': len(new_candidates),
                'new_candidates': new_candidates,
                'status': 'success'
            }
            
            response = requests.post(self.n8n_webhook_url, json=payload, timeout=30)
            
            if response.status_code in [200, 201, 204]:
                print(f"✓ Sent {len(new_candidates)} to n8n")
            else:
                print(f"⚠ Webhook status {response.status_code}")
                
        except Exception as e:
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
        except Exception as e:
            print(f"✗ CSV error: {e}")

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        print("✓ Browser closed")

    async def run(self, headless=True):
        try:
            await self.initialize_browser(headless=headless)
            
            # Load or create session
            session_loaded = await self.load_session()
            
            if not session_loaded:
                if not await self.login_with_retry(max_attempts=3):
                    print("✗ Login failed")
                    return
                await self.save_session()
            else:
                print("→ Verifying session...")
                await self.page.goto(self.base_url, wait_until='domcontentloaded')
                await asyncio.sleep(3)
                
                if '/auth/login' in self.page.url:
                    print("   Session expired, re-login...")
                    if not await self.login_with_retry(max_attempts=3):
                        print("✗ Re-login failed")
                        return
                    await self.save_session()
                else:
                    print("   ✓ Session valid")

            await self.scrape_job_role()

            print("\n→ Navigating to applicants...")
            incoming_url = f"{self.base_url}/jobs/{self.job_id}/incoming"

            try:
                await self.page.goto(incoming_url, wait_until='load', timeout=60000)
                await self.wait_for_stable_page()
                await self.page.wait_for_selector('div.col-span-1.overflow-y-auto', timeout=30000)
                print("   ✓ At applicants page")
            except Exception as e:
                print(f"   ✗ Navigation failed: {e}")
                raise

            sections = ['recommended', 'incoming']
            for section in sections:
                if self.page.is_closed():
                    break
                await self.scrape_section(section)

            if not self.candidates:
                print("\n✗ No candidates scraped")
                return

            await self.save_to_json()
            await self.export_to_csv()
            
            new_candidates = self.push_to_airtable()
            self.send_to_n8n_webhook(new_candidates)

            print("\n" + "="*50)
            print("✓ SCRAPING COMPLETE")
            print(f"  Total: {len(self.candidates)}")
            print(f"  New: {len(new_candidates)}")
            print("="*50)

        except Exception as e:
            print(f"\n✗ Fatal error: {e}")
            
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