"""
Data Transformer - Advanced Data Transformation Engine
Enterprise-grade data transformation for Construction Hub
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import logging

class DataTransformer:
    """Advanced data transformation service for Construction Hub"""
    
    def __init__(self):
        self.transformation_rules = {
            'accounts-payable': {
                'field_mappings': {
                    'id': 'payable_id',
                    'amount': 'payable_amount',
                    'due_date': 'payable_due_date',
                    'supplier_id': 'supplier_id',
                    'status': 'payable_status'
                },
                'calculated_fields': {
                    'days_until_due': self._calculate_days_until_due,
                    'amount_cad': self._convert_to_cad,
                    'risk_score': self._calculate_payable_risk_score
                },
                'aggregations': {
                    'total_amount': 'sum',
                    'avg_amount': 'mean',
                    'count': 'count'
                }
            },
            'accounts-receivable': {
                'field_mappings': {
                    'id': 'receivable_id',
                    'amount': 'receivable_amount',
                    'customer_id': 'customer_id',
                    'payment_date': 'expected_payment_date',
                    'status': 'receivable_status'
                },
                'calculated_fields': {
                    'days_overdue': self._calculate_days_overdue,
                    'amount_cad': self._convert_to_cad,
                    'collection_probability': self._calculate_collection_probability
                }
            },
            'cash-flow': {
                'field_mappings': {
                    'id': 'cashflow_id',
                    'date': 'transaction_date',
                    'type': 'flow_type',
                    'amount': 'flow_amount'
                },
                'calculated_fields': {
                    'cumulative_balance': self._calculate_cumulative_balance,
                    'monthly_total': self._calculate_monthly_total,
                    'trend_indicator': self._calculate_trend_indicator
                }
            },
            'project-management': {
                'field_mappings': {
                    'id': 'project_id',
                    'name': 'project_name',
                    'start_date': 'project_start_date',
                    'end_date': 'project_end_date',
                    'budget': 'project_budget',
                    'actual_cost': 'project_actual_cost'
                },
                'calculated_fields': {
                    'budget_variance': self._calculate_budget_variance,
                    'project_duration': self._calculate_project_duration,
                    'completion_percentage': self._calculate_completion_percentage,
                    'cost_per_day': self._calculate_cost_per_day
                }
            }
        }
    
    def transform_microservice_data(self, service_name: str, endpoint: str, data: Any) -> List[Dict[str, Any]]:
        """Transform data from microservice"""
        try:
            if not data:
                return []
            
            # Handle different data structures
            if isinstance(data, dict):
                if 'data' in data:
                    records = data['data'] if isinstance(data['data'], list) else [data['data']]
                else:
                    records = [data]
            elif isinstance(data, list):
                records = data
            else:
                return []
            
            if service_name not in self.transformation_rules:
                logging.warning(f'No transformation rules defined for service: {service_name}')
                return records
            
            rules = self.transformation_rules[service_name]
            transformed_records = []
            
            for record in records:
                transformed_record = self._transform_record(record, rules, service_name)
                if transformed_record:
                    transformed_records.append(transformed_record)
            
            return transformed_records
            
        except Exception as e:
            logging.error(f"Error transforming microservice data: {str(e)}")
            return []
    
    def _transform_record(self, record: Dict[str, Any], rules: Dict[str, Any], service_name: str) -> Optional[Dict[str, Any]]:
        """Transform individual record"""
        try:
            transformed = {}
            
            # Apply field mappings
            field_mappings = rules.get('field_mappings', {})
            for original_field, new_field in field_mappings.items():
                if original_field in record:
                    transformed[new_field] = record[original_field]
                else:
                    transformed[new_field] = None
            
            # Keep original fields not in mappings
            for field, value in record.items():
                if field not in field_mappings:
                    transformed[field] = value
            
            # Apply calculated fields
            calculated_fields = rules.get('calculated_fields', {})
            for field_name, calculation_func in calculated_fields.items():
                try:
                    transformed[field_name] = calculation_func(record, service_name)
                except Exception as e:
                    logging.warning(f"Error calculating field {field_name}: {str(e)}")
                    transformed[field_name] = None
            
            # Add metadata
            transformed['_source_service'] = service_name
            transformed['_transformed_at'] = datetime.utcnow().isoformat()
            transformed['_record_hash'] = self._calculate_record_hash(record)
            
            return transformed
            
        except Exception as e:
            logging.error(f"Error transforming record: {str(e)}")
            return None
    
    def _calculate_days_until_due(self, record: Dict[str, Any], service_name: str) -> Optional[int]:
        """Calculate days until due date"""
        try:
            if 'due_date' not in record or not record['due_date']:
                return None
            
            due_date = datetime.strptime(record['due_date'], '%Y-%m-%d')
            today = datetime.now()
            delta = (due_date - today).days
            
            return delta
            
        except Exception:
            return None
    
    def _calculate_days_overdue(self, record: Dict[str, Any], service_name: str) -> Optional[int]:
        """Calculate days overdue"""
        try:
            if 'payment_date' not in record or not record['payment_date']:
                return None
            
            payment_date = datetime.strptime(record['payment_date'], '%Y-%m-%d')
            today = datetime.now()
            
            if payment_date < today:
                return (today - payment_date).days
            else:
                return 0
                
        except Exception:
            return None
    
    def _convert_to_cad(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Convert amount to CAD (assuming already in CAD for now)"""
        try:
            amount_field = 'amount'
            if amount_field in record and record[amount_field] is not None:
                return float(record[amount_field])
            return None
        except Exception:
            return None
    
    def _calculate_payable_risk_score(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate risk score for payables"""
        try:
            risk_score = 0.0
            
            # Amount factor (higher amounts = higher risk)
            if 'amount' in record and record['amount']:
                amount = float(record['amount'])
                if amount > 100000:
                    risk_score += 0.3
                elif amount > 50000:
                    risk_score += 0.2
                elif amount > 10000:
                    risk_score += 0.1
            
            # Due date factor
            if 'due_date' in record and record['due_date']:
                try:
                    due_date = datetime.strptime(record['due_date'], '%Y-%m-%d')
                    days_until_due = (due_date - datetime.now()).days
                    
                    if days_until_due < 0:  # Overdue
                        risk_score += 0.4
                    elif days_until_due < 7:  # Due within a week
                        risk_score += 0.2
                    elif days_until_due < 30:  # Due within a month
                        risk_score += 0.1
                except ValueError:
                    pass
            
            # Status factor
            if 'status' in record:
                status = record['status'].lower()
                if status == 'overdue':
                    risk_score += 0.3
                elif status == 'pending':
                    risk_score += 0.1
            
            return min(risk_score, 1.0)  # Cap at 1.0
            
        except Exception:
            return None
    
    def _calculate_collection_probability(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate collection probability for receivables"""
        try:
            probability = 1.0
            
            # Days overdue factor
            if 'payment_date' in record and record['payment_date']:
                try:
                    payment_date = datetime.strptime(record['payment_date'], '%Y-%m-%d')
                    days_overdue = (datetime.now() - payment_date).days
                    
                    if days_overdue > 90:
                        probability *= 0.3
                    elif days_overdue > 60:
                        probability *= 0.5
                    elif days_overdue > 30:
                        probability *= 0.7
                    elif days_overdue > 0:
                        probability *= 0.9
                except ValueError:
                    pass
            
            # Amount factor (larger amounts may be harder to collect)
            if 'amount' in record and record['amount']:
                amount = float(record['amount'])
                if amount > 100000:
                    probability *= 0.9
                elif amount > 500000:
                    probability *= 0.8
            
            # Status factor
            if 'status' in record:
                status = record['status'].lower()
                if status == 'paid':
                    probability = 1.0
                elif status == 'overdue':
                    probability *= 0.6
                elif status == 'partial':
                    probability *= 0.8
            
            return max(probability, 0.0)  # Floor at 0.0
            
        except Exception:
            return None
    
    def _calculate_cumulative_balance(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate cumulative balance (would need historical data)"""
        try:
            # This would require access to historical cash flow data
            # For now, return the current amount
            if 'amount' in record and record['amount']:
                return float(record['amount'])
            return None
        except Exception:
            return None
    
    def _calculate_monthly_total(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate monthly total (would need aggregated data)"""
        try:
            # This would require aggregation across records
            # For now, return the current amount
            if 'amount' in record and record['amount']:
                return float(record['amount'])
            return None
        except Exception:
            return None
    
    def _calculate_trend_indicator(self, record: Dict[str, Any], service_name: str) -> Optional[str]:
        """Calculate trend indicator"""
        try:
            if 'type' in record and 'amount' in record:
                flow_type = record['type'].lower()
                amount = float(record['amount'])
                
                if flow_type == 'inflow' and amount > 0:
                    return 'positive'
                elif flow_type == 'outflow' and amount > 0:
                    return 'negative'
                else:
                    return 'neutral'
            return None
        except Exception:
            return None
    
    def _calculate_budget_variance(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate budget variance"""
        try:
            if 'budget' in record and 'actual_cost' in record:
                budget = float(record['budget'])
                actual_cost = float(record['actual_cost'])
                
                if budget > 0:
                    variance = ((actual_cost - budget) / budget) * 100
                    return round(variance, 2)
            return None
        except Exception:
            return None
    
    def _calculate_project_duration(self, record: Dict[str, Any], service_name: str) -> Optional[int]:
        """Calculate project duration in days"""
        try:
            if 'start_date' in record and 'end_date' in record:
                start_date = datetime.strptime(record['start_date'], '%Y-%m-%d')
                end_date = datetime.strptime(record['end_date'], '%Y-%m-%d')
                
                duration = (end_date - start_date).days
                return max(duration, 0)
            return None
        except Exception:
            return None
    
    def _calculate_completion_percentage(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate project completion percentage"""
        try:
            if 'start_date' in record and 'end_date' in record:
                start_date = datetime.strptime(record['start_date'], '%Y-%m-%d')
                end_date = datetime.strptime(record['end_date'], '%Y-%m-%d')
                today = datetime.now()
                
                if today < start_date:
                    return 0.0
                elif today > end_date:
                    return 100.0
                else:
                    total_duration = (end_date - start_date).days
                    elapsed_duration = (today - start_date).days
                    
                    if total_duration > 0:
                        completion = (elapsed_duration / total_duration) * 100
                        return round(min(completion, 100.0), 2)
            return None
        except Exception:
            return None
    
    def _calculate_cost_per_day(self, record: Dict[str, Any], service_name: str) -> Optional[float]:
        """Calculate cost per day"""
        try:
            if 'actual_cost' in record and 'start_date' in record:
                actual_cost = float(record['actual_cost'])
                start_date = datetime.strptime(record['start_date'], '%Y-%m-%d')
                today = datetime.now()
                
                days_elapsed = (today - start_date).days
                if days_elapsed > 0:
                    cost_per_day = actual_cost / days_elapsed
                    return round(cost_per_day, 2)
            return None
        except Exception:
            return None
    
    def _calculate_record_hash(self, record: Dict[str, Any]) -> str:
        """Calculate hash for record to detect changes"""
        try:
            # Create a consistent string representation of the record
            record_str = json.dumps(record, sort_keys=True, default=str)
            return str(hash(record_str))
        except Exception:
            return str(hash(str(record)))
    
    def aggregate_data(self, data: List[Dict[str, Any]], service_name: str, aggregation_type: str) -> Dict[str, Any]:
        """Aggregate transformed data"""
        try:
            if not data:
                return {}
            
            df = pd.DataFrame(data)
            
            if service_name not in self.transformation_rules:
                return {}
            
            rules = self.transformation_rules[service_name]
            aggregations = rules.get('aggregations', {})
            
            result = {}
            
            for field, agg_func in aggregations.items():
                if field in df.columns:
                    if agg_func == 'sum':
                        result[f'{field}_sum'] = df[field].sum()
                    elif agg_func == 'mean':
                        result[f'{field}_avg'] = df[field].mean()
                    elif agg_func == 'count':
                        result[f'{field}_count'] = df[field].count()
                    elif agg_func == 'max':
                        result[f'{field}_max'] = df[field].max()
                    elif agg_func == 'min':
                        result[f'{field}_min'] = df[field].min()
            
            # Add metadata
            result['total_records'] = len(data)
            result['aggregated_at'] = datetime.utcnow().isoformat()
            result['service_name'] = service_name
            
            return result
            
        except Exception as e:
            logging.error(f"Error aggregating data: {str(e)}")
            return {}

