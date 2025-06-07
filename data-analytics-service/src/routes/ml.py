"""
ML Routes - Placeholder
"""

from flask import Blueprint

ml_bp = Blueprint('ml', __name__)

@ml_bp.route('/test')
def test():
    return {'status': 'ml module loaded'}

