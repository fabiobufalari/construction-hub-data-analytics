"""
Construction Hub Data Analytics Service
Advanced ETL, ML, and Analytics Platform for Construction Financial Recovery System

Author: Manus AI - Enterprise Architect
Version: 1.0.0
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO

# Import all blueprints
from src.routes.etl import etl_bp
from src.routes.analytics import analytics_bp
from src.routes.ml import ml_bp
from src.routes.dashboard import dashboard_bp
from src.routes.reports import reports_bp

app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), 'static'))
app.config['SECRET_KEY'] = 'construction-hub-analytics-2025-enterprise'

# Enable CORS for all routes
CORS(app, origins="*")

# Initialize SocketIO for real-time data
socketio = SocketIO(app, cors_allowed_origins="*")

# Register all blueprints
app.register_blueprint(etl_bp, url_prefix='/api/etl')
app.register_blueprint(analytics_bp, url_prefix='/api/analytics')
app.register_blueprint(ml_bp, url_prefix='/api/ml')
app.register_blueprint(dashboard_bp, url_prefix='/api/dashboard')
app.register_blueprint(reports_bp, url_prefix='/api/reports')

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    """Serve static files and SPA routing"""
    static_folder_path = app.static_folder
    if static_folder_path is None:
        return "Static folder not configured", 404

    if path != "" and os.path.exists(os.path.join(static_folder_path, path)):
        return send_from_directory(static_folder_path, path)
    else:
        index_path = os.path.join(static_folder_path, 'index.html')
        if os.path.exists(index_path):
            return send_from_directory(static_folder_path, 'index.html')
        else:
            return {"message": "Construction Hub Data Analytics Service", "version": "1.0.0", "status": "running"}, 200

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "data-analytics", "version": "1.0.0"}, 200

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)

