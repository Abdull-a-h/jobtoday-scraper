# scraper_api.py
from flask import Flask, jsonify, request
import asyncio
from jobtoday_1 import JobTodayWebhookScraper
from threading import Thread

app = Flask(__name__)

def run_scraper_async():
    """Run the scraper in a separate thread"""
    try:
        scraper = JobTodayWebhookScraper()
        asyncio.run(scraper.run(headless=True))
        return {"status": "success", "message": "Scraper completed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Endpoint for n8n to trigger scraping"""
    print("🔔 Scrape triggered by n8n")
    
    # Run scraper in background
    thread = Thread(target=run_scraper_async)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper started in background"
    }), 202

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # Run on port 5000, accessible from network
    app.run(host='0.0.0.0', port=5000)