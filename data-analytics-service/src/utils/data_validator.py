"""
Data Validator - Advanced Data Quality and Validation
Enterprise-grade data validation for Construction Hub
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
import logging

class DataValidator:
    """Advanced data validation service for Construction Hub"""
    
    def __init__(self):
        self.validation_rules = {
            'accounts-payable': {
                'required_fields': ['id', 'amount', 'due_date', 'supplier_id'],
                'field_types': {
                    'id': 'string',
                    'amount': 'numeric',
                    'due_date': 'date',
                    'supplier_id': 'string',
                    'status': 'string'
                },
                'field_ranges': {
                    'amount': {'min': 0, 'max': 10000000}
                },
                'field_patterns': {
                    'status': r'^(pending|approved|paid|cancelled)$'
                }
            },
            'accounts-receivable': {
                'required_fields': ['id', 'amount', 'customer_id'],
                'field_types': {
                    'id': 'string',
                    'amount': 'numeric',
                    'customer_id': 'string',
                    'payment_date': 'date',
                    'status': 'string'
                },
                'field_ranges': {
                    'amount': {'min': 0, 'max': 10000000}
                },
                'field_patterns': {
                    'status': r'^(pending|partial|paid|overdue)$'
                }
            },
            'cash-flow': {
                'required_fields': ['id', 'date', 'type'],
                'field_types': {
                    'id': 'string',
                    'date': 'date',
                    'type': 'string',
                    'amount': 'numeric'
                },
                'field_patterns': {
                    'type': r'^(inflow|outflow)$'
                }
            },
            'project-management': {
                'required_fields': ['id', 'name', 'start_date'],
                'field_types': {
                    'id': 'string',
                    'name': 'string',
                    'start_date': 'date',
                    'end_date': 'date',
                    'budget': 'numeric',
                    'actual_cost': 'numeric'
                },
                'field_ranges': {
                    'budget': {'min': 0, 'max': 100000000},
                    'actual_cost': {'min': 0, 'max': 100000000}
                }
            }
        }
    
    def validate_microservice_data(self, service_name: str, endpoint: str, data: Any) -> Dict[str, Any]:
        """Validate data from microservice"""
        try:
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'records_validated': 0,
                'records_failed': 0
            }
            
            if not data:
                validation_result['is_valid'] = False
                validation_result['errors'].append('No data received')
                return validation_result
            
            # Handle different data structures
            if isinstance(data, dict):
                if 'data' in data:
                    records = data['data'] if isinstance(data['data'], list) else [data['data']]
                else:
                    records = [data]
            elif isinstance(data, list):
                records = data
            else:
                validation_result['is_valid'] = False
                validation_result['errors'].append('Invalid data format')
                return validation_result
            
            # Validate each record
            for i, record in enumerate(records):
                record_validation = self._validate_record(service_name, record, i)
                
                validation_result['records_validated'] += 1
                
                if not record_validation['is_valid']:
                    validation_result['records_failed'] += 1
                    validation_result['errors'].extend(record_validation['errors'])
                
                validation_result['warnings'].extend(record_validation['warnings'])
            
            # Overall validation status
            if validation_result['records_failed'] > 0:
                failure_rate = validation_result['records_failed'] / validation_result['records_validated']
                if failure_rate > 0.1:  # More than 10% failure rate
                    validation_result['is_valid'] = False
            
            return validation_result
            
        except Exception as e:
            logging.error(f"Error validating microservice data: {str(e)}")
            return {
                'is_valid': False,
                'errors': [f'Validation error: {str(e)}'],
                'warnings': [],
                'records_validated': 0,
                'records_failed': 0
            }
    
    def _validate_record(self, service_name: str, record: Dict[str, Any], record_index: int) -> Dict[str, Any]:
        """Validate individual record"""
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        if service_name not in self.validation_rules:
            result['warnings'].append(f'No validation rules defined for service: {service_name}')
            return result
        
        rules = self.validation_rules[service_name]
        
        # Check required fields
        for field in rules.get('required_fields', []):
            if field not in record or record[field] is None or record[field] == '':
                result['is_valid'] = False
                result['errors'].append(f'Record {record_index}: Missing required field "{field}"')
        
        # Check field types
        for field, expected_type in rules.get('field_types', {}).items():
            if field in record and record[field] is not None:
                if not self._validate_field_type(record[field], expected_type):
                    result['is_valid'] = False
                    result['errors'].append(f'Record {record_index}: Field "{field}" has invalid type. Expected {expected_type}')
        
        # Check field ranges
        for field, range_config in rules.get('field_ranges', {}).items():
            if field in record and record[field] is not None:
                try:
                    value = float(record[field])
                    if 'min' in range_config and value < range_config['min']:
                        result['is_valid'] = False
                        result['errors'].append(f'Record {record_index}: Field "{field}" value {value} is below minimum {range_config["min"]}')
                    
                    if 'max' in range_config and value > range_config['max']:
                        result['is_valid'] = False
                        result['errors'].append(f'Record {record_index}: Field "{field}" value {value} exceeds maximum {range_config["max"]}')
                        
                except (ValueError, TypeError):
                    result['is_valid'] = False
                    result['errors'].append(f'Record {record_index}: Field "{field}" is not numeric for range validation')
        
        # Check field patterns
        for field, pattern in rules.get('field_patterns', {}).items():
            if field in record and record[field] is not None:
                if not re.match(pattern, str(record[field])):
                    result['is_valid'] = False
                    result['errors'].append(f'Record {record_index}: Field "{field}" does not match required pattern')
        
        # Business logic validations
        business_validation = self._validate_business_logic(service_name, record, record_index)
        result['errors'].extend(business_validation['errors'])
        result['warnings'].extend(business_validation['warnings'])
        
        if business_validation['errors']:
            result['is_valid'] = False
        
        return result
    
    def _validate_field_type(self, value: Any, expected_type: str) -> bool:
        """Validate field type"""
        try:
            if expected_type == 'string':
                return isinstance(value, str) or (value is not None and str(value))
            
            elif expected_type == 'numeric':
                if isinstance(value, (int, float)):
                    return True
                try:
                    float(value)
                    return True
                except (ValueError, TypeError):
                    return False
            
            elif expected_type == 'date':
                if isinstance(value, datetime):
                    return True
                if isinstance(value, str):
                    # Try to parse common date formats
                    date_formats = [
                        '%Y-%m-%d',
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%d/%m/%Y',
                        '%m/%d/%Y'
                    ]
                    for fmt in date_formats:
                        try:
                            datetime.strptime(value, fmt)
                            return True
                        except ValueError:
                            continue
                return False
            
            elif expected_type == 'boolean':
                return isinstance(value, bool) or str(value).lower() in ['true', 'false', '1', '0']
            
            return True
            
        except Exception:
            return False
    
    def _validate_business_logic(self, service_name: str, record: Dict[str, Any], record_index: int) -> Dict[str, Any]:
        """Validate business logic rules"""
        result = {
            'errors': [],
            'warnings': []
        }
        
        try:
            if service_name == 'accounts-payable':
                # Due date should not be in the past for pending payments
                if record.get('status') == 'pending' and 'due_date' in record:
                    try:
                        due_date = datetime.strptime(record['due_date'], '%Y-%m-%d')
                        if due_date < datetime.now():
                            result['warnings'].append(f'Record {record_index}: Due date is in the past for pending payment')
                    except ValueError:
                        pass
                
                # Amount should be positive
                if 'amount' in record:
                    try:
                        amount = float(record['amount'])
                        if amount <= 0:
                            result['errors'].append(f'Record {record_index}: Payment amount must be positive')
                    except (ValueError, TypeError):
                        pass
            
            elif service_name == 'project-management':
                # End date should be after start date
                if 'start_date' in record and 'end_date' in record:
                    try:
                        start_date = datetime.strptime(record['start_date'], '%Y-%m-%d')
                        end_date = datetime.strptime(record['end_date'], '%Y-%m-%d')
                        if end_date <= start_date:
                            result['errors'].append(f'Record {record_index}: End date must be after start date')
                    except ValueError:
                        pass
                
                # Actual cost should not exceed budget by more than 50%
                if 'budget' in record and 'actual_cost' in record:
                    try:
                        budget = float(record['budget'])
                        actual_cost = float(record['actual_cost'])
                        if actual_cost > budget * 1.5:
                            result['warnings'].append(f'Record {record_index}: Actual cost significantly exceeds budget')
                    except (ValueError, TypeError):
                        pass
            
            elif service_name == 'cash-flow':
                # Cash flow amount should be reasonable
                if 'amount' in record:
                    try:
                        amount = float(record['amount'])
                        if abs(amount) > 1000000:  # More than 1M CAD
                            result['warnings'].append(f'Record {record_index}: Unusually large cash flow amount')
                    except (ValueError, TypeError):
                        pass
            
        except Exception as e:
            result['warnings'].append(f'Record {record_index}: Error in business logic validation: {str(e)}')
        
        return result
    
    def validate_data_completeness(self, data: pd.DataFrame, required_columns: List[str]) -> Dict[str, Any]:
        """Validate data completeness"""
        try:
            result = {
                'is_complete': True,
                'missing_columns': [],
                'null_percentages': {},
                'recommendations': []
            }
            
            # Check for missing columns
            missing_columns = [col for col in required_columns if col not in data.columns]
            if missing_columns:
                result['is_complete'] = False
                result['missing_columns'] = missing_columns
            
            # Calculate null percentages
            for column in data.columns:
                null_percentage = (data[column].isnull().sum() / len(data)) * 100
                result['null_percentages'][column] = round(null_percentage, 2)
                
                if null_percentage > 10:  # More than 10% null values
                    result['recommendations'].append(f'Column "{column}" has {null_percentage:.1f}% null values')
            
            return result
            
        except Exception as e:
            logging.error(f"Error validating data completeness: {str(e)}")
            return {
                'is_complete': False,
                'error': str(e)
            }
    
    def validate_data_consistency(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate data consistency"""
        try:
            result = {
                'is_consistent': True,
                'inconsistencies': [],
                'duplicate_count': 0
            }
            
            # Check for duplicates
            if 'id' in data.columns:
                duplicate_count = data['id'].duplicated().sum()
                result['duplicate_count'] = duplicate_count
                
                if duplicate_count > 0:
                    result['is_consistent'] = False
                    result['inconsistencies'].append(f'Found {duplicate_count} duplicate IDs')
            
            # Check for data type consistency
            for column in data.columns:
                if data[column].dtype == 'object':
                    # Check for mixed types in object columns
                    unique_types = set(type(x).__name__ for x in data[column].dropna())
                    if len(unique_types) > 1:
                        result['inconsistencies'].append(f'Column "{column}" has mixed data types: {unique_types}')
            
            return result
            
        except Exception as e:
            logging.error(f"Error validating data consistency: {str(e)}")
            return {
                'is_consistent': False,
                'error': str(e)
            }

