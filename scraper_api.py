from flask import Flask, jsonify, request
import asyncio
from threading import Thread
import os
from datetime import datetime
import sys
import json

app = Flask(__name__)

# File-based status (persists across crashes)
STATUS_FILE = '/app/data/scraper_status.json'

def load_status():
    """Load status from file"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
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
            json.dump(status_data, f)
    except Exception as e:
        print(f"⚠ Could not save status: {e}")

# Load initial status
last_run_status = load_status()

def run_scraper_async():
    """Run the scraper in a separate thread with its own event loop"""
    global last_run_status
    
    # Update to running status
    last_run_status = {
        "status": "running",
        "message": "Scraper is running...",
        "last_run": datetime.now().isoformat(),
        "start_time": datetime.now().isoformat()
    }
    save_status(last_run_status)
    
    try:
        print("=" * 60)
        print("🚀 SCRAPER STARTING")
        print("=" * 60)
        
        # Import here to avoid issues with async imports at module level
        from jobtoday_1 import JobTodayWebhookScraper
        
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            scraper = JobTodayWebhookScraper()
            loop.run_until_complete(scraper.run(headless=True))
            
            last_run_status = {
                "status": "success",
                "message": "Scraper completed successfully",
                "last_run": datetime.now().isoformat(),
                "start_time": last_run_status.get("start_time"),
                "end_time": datetime.now().isoformat(),
                "candidates_count": len(scraper.candidates) if hasattr(scraper, 'candidates') else 0
            }
            save_status(last_run_status)
            
            print("=" * 60)
            print("✅ SCRAPER COMPLETED SUCCESSFULLY")
            print(f"   Candidates: {last_run_status.get('candidates_count', 0)}")
            print("=" * 60)
            
        except Exception as scraper_error:
            error_msg = str(scraper_error)
            print(f"❌ Scraper execution error: {error_msg}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            
            last_run_status = {
                "status": "error",
                "message": f"Scraper error: {error_msg[:200]}",
                "last_run": datetime.now().isoformat(),
                "start_time": last_run_status.get("start_time"),
                "end_time": datetime.now().isoformat(),
                "error_details": error_msg
            }
            save_status(last_run_status)
            
        finally:
            try:
                loop.close()
            except:
                pass
            
    except Exception as e:
        error_msg = str(e)
        last_run_status = {
            "status": "error",
            "message": f"Fatal error: {error_msg[:200]}",
            "last_run": datetime.now().isoformat(),
            "error_details": error_msg
        }
        save_status(last_run_status)
        
        print(f"❌ Fatal scraper error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    
    print("=" * 60)
    print(f"🏁 SCRAPER FINISHED - Status: {last_run_status['status']}")
    print("=" * 60)

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
            "/logs": "GET - Get recent logs (if available)"
        },
        "current_status": last_run_status
    }), 200

@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Endpoint for n8n to trigger scraping"""
    global last_run_status
    last_run_status = load_status()
    
    print("🔔 Scrape triggered via API")
    
    # Check if already running
    if last_run_status.get("status") == "running":
        return jsonify({
            "status": "already_running",
            "message": "Scraper is already running",
            "started_at": last_run_status.get("start_time")
        }), 429
    
    # Run scraper in background thread
    thread = Thread(target=run_scraper_async, daemon=True)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper started in background",
        "check_status_at": "/status",
        "started_at": datetime.now().isoformat()
    }), 202

@app.route('/status', methods=['GET'])
def status():
    """Get current scraper status"""
    global last_run_status
    last_run_status = load_status()
    return jsonify(last_run_status), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "JobToday Scraper API",
        "python_version": sys.version,
        "timestamp": datetime.now().isoformat()
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
        "last_run": last_run_status.get("last_run")
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)