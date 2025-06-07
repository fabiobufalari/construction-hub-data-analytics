"""
Analytics Routes - Placeholder
"""

from flask import Blueprint

analytics_bp = Blueprint('analytics', __name__)

@analytics_bp.route('/test')
def test():
    return {'status': 'analytics module loaded'}

