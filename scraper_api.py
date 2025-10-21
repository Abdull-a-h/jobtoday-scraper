# scraper_api.py
from flask import Flask, jsonify, request
import asyncio
from jobtoday_1 import JobTodayWebhookScraper
from threading import Thread
import os

app = Flask(__name__)

# Store the last run status
last_run_status = {
    "status": "idle",
    "message": "No scraper run yet",
    "last_run": None
}

def run_scraper_async():
    """Run the scraper in a separate thread"""
    global last_run_status
    try:
        from datetime import datetime
        last_run_status = {
            "status": "running",
            "message": "Scraper is running...",
            "last_run": datetime.now().isoformat()
        }
        
        scraper = JobTodayWebhookScraper()
        asyncio.run(scraper.run(headless=True))
        
        last_run_status = {
            "status": "success",
            "message": "Scraper completed successfully",
            "last_run": datetime.now().isoformat()
        }
        return {"status": "success", "message": "Scraper completed"}
    except Exception as e:
        from datetime import datetime
        last_run_status = {
            "status": "error",
            "message": str(e),
            "last_run": datetime.now().isoformat()
        }
        return {"status": "error", "message": str(e)}

@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Endpoint for n8n to trigger scraping"""
    print("🔔 Scrape triggered by n8n")
    
    # Check if already running
    if last_run_status.get("status") == "running":
        return jsonify({
            "status": "already_running",
            "message": "Scraper is already running"
        }), 429
    
    # Run scraper in background
    thread = Thread(target=run_scraper_async)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper started in background"
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
        "service": "JobToday Scraper API"
    }), 200

@app.route('/', methods=['GET'])
def home():
    """Home endpoint"""
    return jsonify({
        "service": "JobToday Scraper API",
        "endpoints": {
            "/trigger-scrape": "POST - Trigger the scraper",
            "/status": "GET - Get scraper status",
            "/health": "GET - Health check"
        },
        "current_status": last_run_status
    }), 200

if __name__ == '__main__':
    # Get port from environment variable (Render provides this)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)