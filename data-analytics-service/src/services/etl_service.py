"""
ETL Service - Advanced Data Pipeline Management
Enterprise-grade ETL operations for Construction Hub
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import threading
from typing import Dict, List, Any, Optional
from src.models.database import db, DataSource, ETLJob, AnalyticsMetric
from src.utils.data_validator import DataValidator
from src.utils.data_transformer import DataTransformer

class ETLService:
    """Advanced ETL service for Construction Hub data pipeline"""
    
    def __init__(self):
        self.data_validator = DataValidator()
        self.data_transformer = DataTransformer()
        self.microservice_endpoints = {
            'accounts-payable': 'http://localhost:8001/api',
            'accounts-receivable': 'http://localhost:8002/api',
            'authentication': 'http://localhost:8003/api',
            'cash-flow': 'http://localhost:8004/api',
            'company': 'http://localhost:8005/api',
            'create-people': 'http://localhost:8006/api',
            'employee-costs': 'http://localhost:8007/api',
            'project-management': 'http://localhost:8008/api',
            'supplier': 'http://localhost:8009/api',
            'bank-integration': 'http://localhost:8010/api',
            'financial-reports': 'http://localhost:8011/api',
            'communication': 'http://localhost:8012/api',
            'calculation-materials': 'http://localhost:8013/api',
            'construction-integrations': 'http://localhost:8014/api'
        }
    
    def execute_etl_job(self, job_id: int) -> Dict[str, Any]:
        """Execute ETL job asynchronously"""
        def run_job():
            try:
                job = ETLJob.query.get(job_id)
                if not job:
                    logging.error(f"ETL Job {job_id} not found")
                    return
                
                # Update job status
                job.status = 'running'
                job.start_time = datetime.utcnow()
                db.session.commit()
                
                # Get data source
                source = job.data_source
                
                # Execute ETL based on source type
                if source.type == 'microservice':
                    result = self._extract_from_microservice(source, job)
                elif source.type == 'database':
                    result = self._extract_from_database(source, job)
                elif source.type == 'api':
                    result = self._extract_from_api(source, job)
                elif source.type == 'file':
                    result = self._extract_from_file(source, job)
                else:
                    raise ValueError(f"Unsupported source type: {source.type}")
                
                # Update job completion
                job.status = 'completed'
                job.end_time = datetime.utcnow()
                job.records_processed = result.get('records_processed', 0)
                job.records_failed = result.get('records_failed', 0)
                job.set_metadata(result.get('metadata', {}))
                
                # Update source last sync
                source.last_sync = datetime.utcnow()
                
                db.session.commit()
                logging.info(f"ETL Job {job_id} completed successfully")
                
            except Exception as e:
                # Update job failure
                job = ETLJob.query.get(job_id)
                if job:
                    job.status = 'failed'
                    job.end_time = datetime.utcnow()
                    job.error_message = str(e)
                    db.session.commit()
                
                logging.error(f"ETL Job {job_id} failed: {str(e)}")
        
        # Run job in background thread
        thread = threading.Thread(target=run_job)
        thread.start()
        
        return {'status': 'started', 'job_id': job_id}
    
    def _extract_from_microservice(self, source: DataSource, job: ETLJob) -> Dict[str, Any]:
        """Extract data from microservice"""
        try:
            # Parse connection string to get microservice name
            connection_data = json.loads(source.connection_string)
            service_name = connection_data.get('service_name')
            endpoints = connection_data.get('endpoints', [])
            
            if service_name not in self.microservice_endpoints:
                raise ValueError(f"Unknown microservice: {service_name}")
            
            base_url = self.microservice_endpoints[service_name]
            total_records = 0
            failed_records = 0
            extracted_data = {}
            
            # Extract data from each endpoint
            for endpoint in endpoints:
                try:
                    url = f"{base_url}/{endpoint}"
                    response = requests.get(url, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Validate data
                        validation_result = self.data_validator.validate_microservice_data(
                            service_name, endpoint, data
                        )
                        
                        if validation_result['is_valid']:
                            # Transform data
                            transformed_data = self.data_transformer.transform_microservice_data(
                                service_name, endpoint, data
                            )
                            
                            # Store in data warehouse
                            self._store_in_warehouse(service_name, endpoint, transformed_data)
                            
                            extracted_data[endpoint] = {
                                'records': len(transformed_data) if isinstance(transformed_data, list) else 1,
                                'status': 'success'
                            }
                            total_records += extracted_data[endpoint]['records']
                        else:
                            failed_records += 1
                            extracted_data[endpoint] = {
                                'status': 'validation_failed',
                                'errors': validation_result['errors']
                            }
                    else:
                        failed_records += 1
                        extracted_data[endpoint] = {
                            'status': 'http_error',
                            'status_code': response.status_code
                        }
                        
                except requests.RequestException as e:
                    failed_records += 1
                    extracted_data[endpoint] = {
                        'status': 'connection_error',
                        'error': str(e)
                    }
            
            return {
                'records_processed': total_records,
                'records_failed': failed_records,
                'metadata': {
                    'service_name': service_name,
                    'endpoints_processed': len(endpoints),
                    'extraction_details': extracted_data
                }
            }
            
        except Exception as e:
            logging.error(f"Error extracting from microservice: {str(e)}")
            raise
    
    def _extract_from_database(self, source: DataSource, job: ETLJob) -> Dict[str, Any]:
        """Extract data from database"""
        try:
            connection_data = json.loads(source.connection_string)
            # Implementation for database extraction
            # This would connect to external databases and extract data
            
            return {
                'records_processed': 0,
                'records_failed': 0,
                'metadata': {'type': 'database', 'status': 'not_implemented'}
            }
            
        except Exception as e:
            logging.error(f"Error extracting from database: {str(e)}")
            raise
    
    def _extract_from_api(self, source: DataSource, job: ETLJob) -> Dict[str, Any]:
        """Extract data from external API"""
        try:
            connection_data = json.loads(source.connection_string)
            # Implementation for API extraction
            
            return {
                'records_processed': 0,
                'records_failed': 0,
                'metadata': {'type': 'api', 'status': 'not_implemented'}
            }
            
        except Exception as e:
            logging.error(f"Error extracting from API: {str(e)}")
            raise
    
    def _extract_from_file(self, source: DataSource, job: ETLJob) -> Dict[str, Any]:
        """Extract data from file"""
        try:
            connection_data = json.loads(source.connection_string)
            # Implementation for file extraction
            
            return {
                'records_processed': 0,
                'records_failed': 0,
                'metadata': {'type': 'file', 'status': 'not_implemented'}
            }
            
        except Exception as e:
            logging.error(f"Error extracting from file: {str(e)}")
            raise
    
    def _store_in_warehouse(self, service_name: str, endpoint: str, data: Any) -> None:
        """Store transformed data in data warehouse"""
        try:
            # Convert data to analytics metrics
            if isinstance(data, list):
                for record in data:
                    self._create_analytics_metrics(service_name, endpoint, record)
            else:
                self._create_analytics_metrics(service_name, endpoint, data)
                
        except Exception as e:
            logging.error(f"Error storing in warehouse: {str(e)}")
            raise
    
    def _create_analytics_metrics(self, service_name: str, endpoint: str, record: Dict[str, Any]) -> None:
        """Create analytics metrics from record data"""
        try:
            # Define metric mappings for different services
            metric_mappings = {
                'accounts-payable': {
                    'amount': ('financial', 'CAD'),
                    'due_date': ('operational', 'date'),
                    'status': ('operational', 'status')
                },
                'accounts-receivable': {
                    'amount': ('financial', 'CAD'),
                    'payment_date': ('operational', 'date'),
                    'status': ('operational', 'status')
                },
                'cash-flow': {
                    'inflow': ('financial', 'CAD'),
                    'outflow': ('financial', 'CAD'),
                    'balance': ('financial', 'CAD')
                },
                'project-management': {
                    'budget': ('financial', 'CAD'),
                    'actual_cost': ('financial', 'CAD'),
                    'progress': ('operational', 'percentage')
                }
            }
            
            if service_name in metric_mappings:
                mappings = metric_mappings[service_name]
                
                for field, (category, unit) in mappings.items():
                    if field in record and record[field] is not None:
                        try:
                            value = float(record[field]) if isinstance(record[field], (int, float, str)) else 0
                            
                            metric = AnalyticsMetric(
                                metric_name=f"{service_name}_{endpoint}_{field}",
                                metric_category=category,
                                metric_value=value,
                                metric_unit=unit,
                                dimension_1=record.get('id', ''),
                                dimension_2=record.get('company_id', ''),
                                dimension_3=record.get('project_id', '')
                            )
                            
                            db.session.add(metric)
                            
                        except (ValueError, TypeError):
                            continue
            
            db.session.commit()
            
        except Exception as e:
            logging.error(f"Error creating analytics metrics: {str(e)}")
            db.session.rollback()
    
    def generate_data_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report"""
        try:
            # Get recent ETL jobs
            recent_jobs = ETLJob.query.filter(
                ETLJob.start_time >= datetime.utcnow() - timedelta(days=7)
            ).all()
            
            # Calculate quality metrics
            total_jobs = len(recent_jobs)
            successful_jobs = len([job for job in recent_jobs if job.status == 'completed'])
            failed_jobs = len([job for job in recent_jobs if job.status == 'failed'])
            
            total_records = sum(job.records_processed for job in recent_jobs if job.records_processed)
            failed_records = sum(job.records_failed for job in recent_jobs if job.records_failed)
            
            success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0
            data_quality_rate = ((total_records - failed_records) / total_records * 100) if total_records > 0 else 0
            
            # Get data source statistics
            sources = DataSource.query.all()
            active_sources = len([s for s in sources if s.is_active])
            
            # Get recent metrics count
            recent_metrics = AnalyticsMetric.query.filter(
                AnalyticsMetric.created_at >= datetime.utcnow() - timedelta(days=7)
            ).count()
            
            return {
                'report_date': datetime.utcnow().isoformat(),
                'period': '7_days',
                'job_statistics': {
                    'total_jobs': total_jobs,
                    'successful_jobs': successful_jobs,
                    'failed_jobs': failed_jobs,
                    'success_rate': round(success_rate, 2)
                },
                'data_statistics': {
                    'total_records_processed': total_records,
                    'failed_records': failed_records,
                    'data_quality_rate': round(data_quality_rate, 2)
                },
                'source_statistics': {
                    'total_sources': len(sources),
                    'active_sources': active_sources,
                    'inactive_sources': len(sources) - active_sources
                },
                'metrics_generated': recent_metrics,
                'recommendations': self._generate_quality_recommendations(
                    success_rate, data_quality_rate, failed_jobs
                )
            }
            
        except Exception as e:
            logging.error(f"Error generating data quality report: {str(e)}")
            raise
    
    def _generate_quality_recommendations(self, success_rate: float, data_quality_rate: float, failed_jobs: int) -> List[str]:
        """Generate data quality recommendations"""
        recommendations = []
        
        if success_rate < 90:
            recommendations.append("Job success rate is below 90%. Review failed jobs and improve error handling.")
        
        if data_quality_rate < 95:
            recommendations.append("Data quality rate is below 95%. Implement stricter validation rules.")
        
        if failed_jobs > 5:
            recommendations.append("High number of failed jobs detected. Check data source connectivity and configurations.")
        
        if not recommendations:
            recommendations.append("Data quality metrics are within acceptable ranges. Continue monitoring.")
        
        return recommendations

