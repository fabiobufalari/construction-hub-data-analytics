"""
ETL (Extract, Transform, Load) Routes
Advanced data pipeline management for Construction Hub
"""

from flask import Blueprint, request, jsonify
import logging

etl_bp = Blueprint('etl', __name__)

@etl_bp.route('/sources', methods=['GET'])
def get_data_sources():
    """Get all configured data sources"""
    try:
        # Mock data for now
        sources = [
            {
                'id': 1,
                'name': 'accounts-payable-service',
                'type': 'microservice',
                'is_active': True,
                'last_sync': '2025-06-07T05:00:00Z',
                'created_at': '2025-06-01T00:00:00Z'
            },
            {
                'id': 2,
                'name': 'accounts-receivable-service',
                'type': 'microservice',
                'is_active': True,
                'last_sync': '2025-06-07T05:00:00Z',
                'created_at': '2025-06-01T00:00:00Z'
            }
        ]
        
        return jsonify({
            'success': True,
            'data': sources
        }), 200
    except Exception as e:
        logging.error(f"Error fetching data sources: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@etl_bp.route('/test', methods=['GET'])
def test_etl():
    """Test ETL endpoint"""
    return jsonify({
        'success': True,
        'message': 'ETL service is running',
        'version': '1.0.0'
    }), 200

