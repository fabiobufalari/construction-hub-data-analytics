"""
Reports Routes - Placeholder
"""

from flask import Blueprint

reports_bp = Blueprint('reports', __name__)

@reports_bp.route('/test')
def test():
    return {'status': 'reports module loaded'}

