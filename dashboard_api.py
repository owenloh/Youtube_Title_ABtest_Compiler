"""Flask API for dashboard - health and status tracking."""
from datetime import datetime, date
from flask import Flask, jsonify, send_file
from flask_cors import CORS

from storage import get_all_videos_summary, get_video_info, init_db

app = Flask(__name__)
CORS(app)  # Allow frontend to call from any origin


@app.route("/", methods=["GET"])
def dashboard():
    """Serve the dashboard HTML."""
    return send_file("dashboard.html")


def serialize_value(value):
    """Convert datetime/date objects to ISO strings for JSON."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


@app.route("/api/reset", methods=["POST"])
def reset_database():
    """Clear all data from database. Use with caution!"""
    try:
        from storage import get_conn, get_pool
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM title_samples;")
        cur.execute("DELETE FROM title_history;")
        cur.execute("DELETE FROM videos;")
        cur.execute("DELETE FROM channels;")
        conn.commit()
        cur.close()
        get_pool().putconn(conn)
        return jsonify({"status": "ok", "message": "Database cleared"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.route("/api/videos", methods=["GET"])
def get_videos():
    """Get summary of all videos."""
    try:
        videos = get_all_videos_summary()
        # Convert datetime/date objects to ISO strings for JSON
        for video in videos:
            for key, value in video.items():
                video[key] = serialize_value(value)
        return jsonify({"videos": videos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/video/<video_id>", methods=["GET"])
def get_video(video_id: str):
    """Get detailed info for a specific video."""
    try:
        video = get_video_info(video_id)
        if not video:
            return jsonify({"error": "Video not found"}), 404
        
        # Convert datetime/date objects
        for key, value in video.items():
            video[key] = serialize_value(value)
        
        return jsonify({"video": video})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Get overall statistics."""
    try:
        videos = get_all_videos_summary()
        total_videos = len(videos)
        active_videos = sum(1 for v in videos if not v.get("is_ignored") and not v.get("is_short") and not v.get("is_deleted"))
        ignored_videos = sum(1 for v in videos if v.get("is_ignored"))
        deleted_videos = sum(1 for v in videos if v.get("is_deleted"))
        shorts = sum(1 for v in videos if v.get("is_short"))
        videos_with_comments = sum(1 for v in videos if v.get("comment_id"))
        
        return jsonify({
            "total_videos": total_videos,
            "active_videos": active_videos,
            "ignored_videos": ignored_videos,
            "deleted_videos": deleted_videos,
            "shorts": shorts,
            "videos_with_comments": videos_with_comments,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)
