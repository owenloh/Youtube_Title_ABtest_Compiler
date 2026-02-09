"""
Combined entry point: runs both the scheduler and dashboard API.
- Dashboard API runs on PORT (default 5000)
- Scheduler runs in a background thread
"""
import os
import threading

from dashboard_api import app, init_db
from main import run_scheduler


def start_scheduler():
    """Run scheduler in background thread."""
    print("Starting scheduler in background thread...")
    run_scheduler()


if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Run Flask app (blocks main thread)
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting dashboard API on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
