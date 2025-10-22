from flask import Flask, jsonify, request
import asyncio
from threading import Thread
import os
from datetime import datetime
import sys
import json
import traceback
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# File-based status (persists across crashes)
STATUS_FILE = '/app/data/scraper_status.json'
HEARTBEAT_FILE = '/app/data/scraper_heartbeat.json'

# Global variable to track if scraper is running
scraper_thread = None

def load_status():
    """Load status from file"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading status: {e}")
    return {
        "status": "idle",
        "message": "No scraper run yet",
        "last_run": None
    }

def save_status(status_data):
    """Save status to file"""
    try:
        os.makedirs('/app/data', exist_ok=True)
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_data, f, indent=2)
        logger.info(f"Status saved: {status_data['status']}")
    except Exception as e:
        logger.error(f"Could not save status: {e}")

def update_heartbeat():
    """Update heartbeat file to show scraper is alive"""
    try:
        os.makedirs('/app/data', exist_ok=True)
        heartbeat_data = {
            'timestamp': datetime.now().isoformat(),
            'status': 'alive'
        }
        with open(HEARTBEAT_FILE, 'w') as f:
            json.dump(heartbeat_data, f)
    except Exception as e:
        logger.error(f"Could not update heartbeat: {e}")

def get_heartbeat():
    """Get last heartbeat timestamp"""
    try:
        if os.path.exists(HEARTBEAT_FILE):
            with open(HEARTBEAT_FILE, 'r') as f:
                data = json.load(f)
                return data.get('timestamp')
    except:
        pass
    return None

# Load initial status
last_run_status = load_status()

class ScraperProgress:
    """Track scraper progress and update periodically"""
    def __init__(self):
        self.current_section = None
        self.current_candidate = None
        self.total_candidates = 0
        self.processed_count = 0
        self.last_update = time.time()
        
    def update(self, section=None, candidate=None, total=None, processed=None):
        """Update progress"""
        if section:
            self.current_section = section
        if candidate:
            self.current_candidate = candidate
        if total is not None:
            self.total_candidates = total
        if processed is not None:
            self.processed_count = processed
            
        # Update heartbeat every 10 seconds
        current_time = time.time()
        if current_time - self.last_update > 10:
            update_heartbeat()
            self.last_update = current_time
            
            # Also update status with progress
            global last_run_status
            status_update = last_run_status.copy()
            status_update['progress'] = {
                'section': self.current_section,
                'candidate': self.current_candidate,
                'processed': self.processed_count,
                'total': self.total_candidates
            }
            save_status(status_update)
            logger.info(f"Progress: {self.processed_count}/{self.total_candidates} candidates")

progress_tracker = ScraperProgress()

def run_scraper_async():
    """Run the scraper in a separate thread with its own event loop"""
    global last_run_status, progress_tracker
    
    # Update to running status
    last_run_status = {
        "status": "running",
        "message": "Scraper is running...",
        "last_run": datetime.now().isoformat(),
        "start_time": datetime.now().isoformat(),
        "progress": {
            "section": None,
            "candidate": None,
            "processed": 0,
            "total": 0
        }
    }
    save_status(last_run_status)
    update_heartbeat()
    
    try:
        logger.info("=" * 60)
        logger.info("🚀 SCRAPER STARTING")
        logger.info("=" * 60)
        
        # Import here to avoid issues with async imports at module level
        from jobtoday_1 import JobTodayWebhookScraper
        
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            logger.info("Initializing scraper...")
            scraper = JobTodayWebhookScraper()
            
            # Pass progress tracker to scraper
            scraper.progress_tracker = progress_tracker
            
            logger.info("Starting scraper.run()...")
            loop.run_until_complete(scraper.run(headless=True))
            
            candidates_count = len(scraper.candidates) if hasattr(scraper, 'candidates') else 0
            
            last_run_status = {
                "status": "success",
                "message": "Scraper completed successfully",
                "last_run": datetime.now().isoformat(),
                "start_time": last_run_status.get("start_time"),
                "end_time": datetime.now().isoformat(),
                "candidates_count": candidates_count
            }
            save_status(last_run_status)
            
            logger.info("=" * 60)
            logger.info(f"✅ SCRAPER COMPLETED SUCCESSFULLY")
            logger.info(f"   Candidates: {candidates_count}")
            logger.info("=" * 60)
            
        except Exception as scraper_error:
            error_msg = str(scraper_error)
            error_trace = traceback.format_exc()
            
            logger.error("=" * 60)
            logger.error(f"❌ SCRAPER EXECUTION ERROR")
            logger.error(f"Error: {error_msg}")
            logger.error("Traceback:")
            logger.error(error_trace)
            logger.error("=" * 60)
            
            last_run_status = {
                "status": "error",
                "message": f"Scraper error: {error_msg[:200]}",
                "last_run": datetime.now().isoformat(),
                "start_time": last_run_status.get("start_time"),
                "end_time": datetime.now().isoformat(),
                "error_details": error_msg,
                "error_traceback": error_trace[:1000]
            }
            save_status(last_run_status)
            
        finally:
            try:
                loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}")
            
    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()
        
        logger.error("=" * 60)
        logger.error(f"❌ FATAL SCRAPER ERROR")
        logger.error(f"Error: {error_msg}")
        logger.error("Traceback:")
        logger.error(error_trace)
        logger.error("=" * 60)
        
        last_run_status = {
            "status": "error",
            "message": f"Fatal error: {error_msg[:200]}",
            "last_run": datetime.now().isoformat(),
            "error_details": error_msg,
            "error_traceback": error_trace[:1000]
        }
        save_status(last_run_status)
    
    logger.info("=" * 60)
    logger.info(f"🏁 SCRAPER FINISHED - Status: {last_run_status['status']}")
    logger.info("=" * 60)

@app.route('/', methods=['GET', 'HEAD'])
def home():
    """Home endpoint"""
    if request.method == 'HEAD':
        return '', 200
    
    # Reload status from file in case it was updated
    global last_run_status
    last_run_status = load_status()
    
    return jsonify({
        "service": "JobToday Scraper API",
        "status": "operational",
        "endpoints": {
            "/trigger-scrape": "POST - Trigger the scraper",
            "/status": "GET - Get scraper status",
            "/health": "GET - Health check",
            "/logs": "GET - Get recent logs (if available)",
            "/heartbeat": "GET - Get scraper heartbeat"
        },
        "current_status": last_run_status
    }), 200

@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Endpoint for n8n to trigger scraping"""
    global last_run_status, scraper_thread
    last_run_status = load_status()
    
    logger.info("🔔 Scrape triggered via API")
    
    # Check if already running
    if last_run_status.get("status") == "running":
        # Check if thread is actually alive
        if scraper_thread and scraper_thread.is_alive():
            logger.warning("Scraper already running, rejecting new request")
            return jsonify({
                "status": "already_running",
                "message": "Scraper is already running",
                "started_at": last_run_status.get("start_time"),
                "progress": last_run_status.get("progress")
            }), 429
        else:
            # Thread died but status wasn't updated
            logger.warning("Scraper thread died without updating status, allowing new run")
    
    # Run scraper in background thread
    logger.info("Starting scraper in background thread...")
    scraper_thread = Thread(target=run_scraper_async, daemon=False)
    scraper_thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper started in background",
        "check_status_at": "/status",
        "check_heartbeat_at": "/heartbeat",
        "started_at": datetime.now().isoformat()
    }), 202

@app.route('/status', methods=['GET'])
def status():
    """Get current scraper status"""
    global last_run_status
    last_run_status = load_status()
    return jsonify(last_run_status), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    """Get scraper heartbeat - shows if scraper is alive"""
    last_heartbeat = get_heartbeat()
    is_alive = False
    seconds_since_heartbeat = None
    
    if last_heartbeat:
        try:
            heartbeat_time = datetime.fromisoformat(last_heartbeat)
            seconds_since_heartbeat = (datetime.now() - heartbeat_time).total_seconds()
            is_alive = seconds_since_heartbeat < 30  # Consider alive if heartbeat within 30 seconds
        except:
            pass
    
    return jsonify({
        "last_heartbeat": last_heartbeat,
        "seconds_since_heartbeat": seconds_since_heartbeat,
        "is_alive": is_alive,
        "status": "healthy" if is_alive else "no_recent_heartbeat"
    }), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint - always responds quickly"""
    return jsonify({
        "status": "healthy",
        "service": "JobToday Scraper API",
        "python_version": sys.version,
        "timestamp": datetime.now().isoformat(),
        "playwright_installed": check_playwright()
    }), 200

@app.route('/logs', methods=['GET'])
def logs():
    """Get recent error logs if available"""
    global last_run_status
    last_run_status = load_status()
    
    return jsonify({
        "status": last_run_status.get("status"),
        "message": last_run_status.get("message"),
        "error_details": last_run_status.get("error_details", "No error details available"),
        "error_traceback": last_run_status.get("error_traceback", "No traceback available"),
        "last_run": last_run_status.get("last_run"),
        "progress": last_run_status.get("progress")
    }), 200

def check_playwright():
    """Check if Playwright is properly installed"""
    try:
        from playwright.sync_api import sync_playwright
        return True
    except Exception as e:
        logger.error(f"Playwright check failed: {e}")
        return False

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)