"""
Dashboard Routes - Placeholder
"""

from flask import Blueprint

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/test')
def test():
    return {'status': 'dashboard module loaded'}

