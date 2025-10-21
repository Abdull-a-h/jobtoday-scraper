from flask import Flask, jsonify, request
import asyncio
from threading import Thread
import os
from datetime import datetime
import sys

app = Flask(__name__)

# Store the last run status
last_run_status = {
    "status": "idle",
    "message": "No scraper run yet",
    "last_run": None
}

def run_scraper_async():
    """Run the scraper in a separate thread with its own event loop"""
    global last_run_status
    try:
        last_run_status = {
            "status": "running",
            "message": "Scraper is running...",
            "last_run": datetime.now().isoformat()
        }
        
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
                "candidates_count": len(scraper.candidates) if hasattr(scraper, 'candidates') else 0
            }
        finally:
            loop.close()
            
    except Exception as e:
        last_run_status = {
            "status": "error",
            "message": str(e),
            "last_run": datetime.now().isoformat()
        }
        print(f"❌ Scraper error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()

@app.route('/', methods=['GET', 'HEAD'])
def home():
    """Home endpoint"""
    if request.method == 'HEAD':
        return '', 200
        
    return jsonify({
        "service": "JobToday Scraper API",
        "status": "operational",
        "endpoints": {
            "/trigger-scrape": "POST - Trigger the scraper",
            "/status": "GET - Get scraper status",
            "/health": "GET - Health check"
        },
        "current_status": last_run_status
    }), 200

@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Endpoint for n8n to trigger scraping"""
    print("🔔 Scrape triggered")
    
    # Check if already running
    if last_run_status.get("status") == "running":
        return jsonify({
            "status": "already_running",
            "message": "Scraper is already running"
        }), 429
    
    # Run scraper in background thread
    thread = Thread(target=run_scraper_async, daemon=True)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper started in background",
        "check_status_at": "/status"
    }), 202

@app.route('/status', methods=['GET'])
def status():
    """Get current scraper status"""
    return jsonify(last_run_status), 200

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "JobToday Scraper API",
        "python_version": sys.version
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)