"""
Database Models for Construction Hub Data Analytics Service
Enterprise-grade data models for analytics and ML
"""

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json

db = SQLAlchemy()

class DataSource(db.Model):
    """Data source configuration for ETL pipeline"""
    __tablename__ = 'data_sources'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    type = db.Column(db.String(50), nullable=False)  # microservice, database, api, file
    connection_string = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    last_sync = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    etl_jobs = db.relationship('ETLJob', backref='data_source', lazy=True)

class ETLJob(db.Model):
    """ETL job execution tracking"""
    __tablename__ = 'etl_jobs'
    
    id = db.Column(db.Integer, primary_key=True)
    data_source_id = db.Column(db.Integer, db.ForeignKey('data_sources.id'), nullable=False)
    job_name = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), nullable=False)  # pending, running, completed, failed
    records_processed = db.Column(db.Integer, default=0)
    records_failed = db.Column(db.Integer, default=0)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime)
    error_message = db.Column(db.Text)
    job_metadata = db.Column(db.Text)  # JSON metadata
    
    def set_metadata(self, data):
        self.job_metadata = json.dumps(data)
    
    def get_metadata(self):
        return json.loads(self.job_metadata) if self.job_metadata else {}

class AnalyticsMetric(db.Model):
    """Analytics metrics storage"""
    __tablename__ = 'analytics_metrics'
    
    id = db.Column(db.Integer, primary_key=True)
    metric_name = db.Column(db.String(100), nullable=False)
    metric_category = db.Column(db.String(50), nullable=False)  # financial, operational, project, risk
    metric_value = db.Column(db.Float, nullable=False)
    metric_unit = db.Column(db.String(20))
    dimension_1 = db.Column(db.String(100))  # project_id, company_id, etc.
    dimension_2 = db.Column(db.String(100))
    dimension_3 = db.Column(db.String(100))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class MLModel(db.Model):
    """Machine Learning model registry"""
    __tablename__ = 'ml_models'
    
    id = db.Column(db.Integer, primary_key=True)
    model_name = db.Column(db.String(100), nullable=False, unique=True)
    model_type = db.Column(db.String(50), nullable=False)  # regression, classification, clustering, forecasting
    model_version = db.Column(db.String(20), nullable=False)
    model_path = db.Column(db.String(255), nullable=False)
    accuracy_score = db.Column(db.Float)
    training_data_size = db.Column(db.Integer)
    features = db.Column(db.Text)  # JSON list of features
    target_variable = db.Column(db.String(100))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    predictions = db.relationship('MLPrediction', backref='model', lazy=True)

class MLPrediction(db.Model):
    """ML prediction results storage"""
    __tablename__ = 'ml_predictions'
    
    id = db.Column(db.Integer, primary_key=True)
    model_id = db.Column(db.Integer, db.ForeignKey('ml_models.id'), nullable=False)
    input_data = db.Column(db.Text, nullable=False)  # JSON input features
    prediction_value = db.Column(db.Float, nullable=False)
    confidence_score = db.Column(db.Float)
    prediction_type = db.Column(db.String(50))  # cash_flow, risk_score, project_cost, etc.
    entity_id = db.Column(db.String(100))  # project_id, company_id, etc.
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Dashboard(db.Model):
    """Dashboard configuration storage"""
    __tablename__ = 'dashboards'
    
    id = db.Column(db.Integer, primary_key=True)
    dashboard_name = db.Column(db.String(100), nullable=False)
    dashboard_type = db.Column(db.String(50), nullable=False)  # executive, financial, operational, project
    user_id = db.Column(db.String(100))
    configuration = db.Column(db.Text, nullable=False)  # JSON dashboard config
    is_public = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Report(db.Model):
    """Generated reports storage"""
    __tablename__ = 'reports'
    
    id = db.Column(db.Integer, primary_key=True)
    report_name = db.Column(db.String(100), nullable=False)
    report_type = db.Column(db.String(50), nullable=False)  # financial, operational, compliance, custom
    report_format = db.Column(db.String(20), nullable=False)  # pdf, excel, json, html
    file_path = db.Column(db.String(255))
    parameters = db.Column(db.Text)  # JSON report parameters
    generated_by = db.Column(db.String(100))
    status = db.Column(db.String(20), default='pending')  # pending, generating, completed, failed
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

class Alert(db.Model):
    """System alerts and notifications"""
    __tablename__ = 'alerts'
    
    id = db.Column(db.Integer, primary_key=True)
    alert_type = db.Column(db.String(50), nullable=False)  # threshold, anomaly, system, business
    severity = db.Column(db.String(20), nullable=False)  # low, medium, high, critical
    title = db.Column(db.String(200), nullable=False)
    message = db.Column(db.Text, nullable=False)
    entity_type = db.Column(db.String(50))  # project, company, financial, system
    entity_id = db.Column(db.String(100))
    is_read = db.Column(db.Boolean, default=False)
    is_resolved = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    resolved_at = db.Column(db.DateTime)

