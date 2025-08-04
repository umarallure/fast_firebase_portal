"""
Lead Search Service with Fuzzy Name Matching
Searches leads by name with intelligent matching algorithms
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional, Tuple
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import logging

logger = logging.getLogger(__name__)

class LeadSearchService:
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.leads_data: List[Dict[str, Any]] = []
        
    def load_csv_data(self, csv_content: str) -> bool:
        """Load CSV data from string content"""
        try:
            from io import StringIO
            self.df = pd.read_csv(StringIO(csv_content))
            self.df = self.df.fillna('')  # Replace NaN with empty string
            
            # Convert to list of dictionaries for easier processing
            self.leads_data = self.df.to_dict('records')
            
            logger.info(f"Loaded {len(self.leads_data)} leads from CSV")
            return True
            
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            return False
    
    def normalize_name(self, name: str) -> str:
        """Normalize name for better matching"""
        if not name:
            return ""
        
        # Remove extra spaces, convert to lowercase
        name = re.sub(r'\s+', ' ', name.strip().lower())
        
        # Remove common prefixes/suffixes
        name = re.sub(r'\b(mr|mrs|ms|dr|prof|sr|jr|i{1,3})\b\.?', '', name)
        
        # Remove special characters except spaces and hyphens
        name = re.sub(r'[^\w\s\-]', '', name)
        
        return name.strip()
    
    def split_name_parts(self, name: str) -> List[str]:
        """Split name into parts for better matching"""
        normalized = self.normalize_name(name)
        parts = [part for part in normalized.split() if len(part) > 1]
        return parts
    
    def calculate_name_similarity(self, search_name: str, lead_name: str) -> Tuple[int, Dict[str, int]]:
        """Calculate similarity score between search name and lead name"""
        search_normalized = self.normalize_name(search_name)
        lead_normalized = self.normalize_name(lead_name)
        
        # Direct fuzzy match
        direct_score = fuzz.ratio(search_normalized, lead_normalized)
        
        # Token sort ratio (handles word order differences)
        token_sort_score = fuzz.token_sort_ratio(search_normalized, lead_normalized)
        
        # Partial ratio (handles partial matches)
        partial_score = fuzz.partial_ratio(search_normalized, lead_normalized)
        
        # Token set ratio (handles different word sets)
        token_set_score = fuzz.token_set_ratio(search_normalized, lead_normalized)
        
        # Custom logic for first/last name swaps
        search_parts = self.split_name_parts(search_name)
        lead_parts = self.split_name_parts(lead_name)
        
        swap_score = 0
        if len(search_parts) >= 2 and len(lead_parts) >= 2:
            # Try swapping first and last names
            swapped_search = ' '.join([search_parts[-1]] + search_parts[:-1])
            swap_score = fuzz.ratio(swapped_search, lead_normalized)
        
        # Calculate final score (weighted combination)
        scores = {
            'direct': direct_score,
            'token_sort': token_sort_score,
            'partial': partial_score,
            'token_set': token_set_score,
            'swap': swap_score
        }
        
        # Weighted final score
        final_score = max(
            direct_score,
            token_sort_score,
            partial_score * 0.9,
            token_set_score * 0.95,
            swap_score * 0.85
        )
        
        return int(final_score), scores
    
    def search_leads(self, search_name: str, min_score: int = 60, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search for leads by name with fuzzy matching"""
        if not self.leads_data:
            return []
        
        if not search_name or len(search_name.strip()) < 2:
            return []
        
        results = []
        
        for lead in self.leads_data:
            full_name = str(lead.get('full_name', '')).strip()
            if not full_name:
                continue
            
            # Calculate similarity score
            score, score_details = self.calculate_name_similarity(search_name, full_name)
            
            if score >= min_score:
                # Extract the required fields
                result = {
                    'full_name': full_name,
                    'draft_date': lead.get('Draft_Date', lead.get('draft_date', '')),
                    'monthly_premium': lead.get('Monthly Premium', lead.get('monthly_premium', '')),
                    'coverage_amount': lead.get('Coverage Amount', lead.get('coverage_amount', '')),
                    'carrier': lead.get('Carrier', lead.get('carrier', '')),
                    'call_center': lead.get('center', ''),
                    'similarity_score': score,
                    'score_details': score_details,
                    'phone': lead.get('phone', ''),
                    'email': lead.get('email', ''),
                    'address': lead.get('address', ''),
                    'city': lead.get('city', ''),
                    'state': lead.get('state', ''),
                    'postal_code': lead.get('postal_code', ''),
                    'beneficiary_information': lead.get('Beneficiary_Information', lead.get('beneficiary_information', '')),
                    'tobacco_user': lead.get('custom_Tobacco User?', ''),
                    'health_conditions': lead.get('Health_Conditions', ''),
                    'medications': lead.get('Medications', ''),
                    'age': lead.get('Age', ''),
                    'height': lead.get('Height', ''),
                    'weight': lead.get('Weight', ''),
                    'doctors_name': lead.get('Doctors_Name', ''),
                    'routing_number': lead.get('Routing #', lead.get('routing_number', '')),
                    'account_number': lead.get('Account #', lead.get('account_number', '')),
                }
                
                results.append(result)
        
        # Sort by similarity score (highest first)
        results.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        # Return top results
        return results[:max_results]
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get statistics about the loaded database"""
        if self.df is None:
            return {
                'total_leads': 0,
                'columns': []
            }
        
        stats = {
            'total_leads': len(self.df),
            'columns': list(self.df.columns),
            'carriers': self.df['Carrier'].value_counts().head(10).to_dict() if 'Carrier' in self.df.columns else {},
            'call_centers': self.df['center'].value_counts().head(10).to_dict() if 'center' in self.df.columns else {},
            'sample_names': self.df['full_name'].dropna().head(5).tolist() if 'full_name' in self.df.columns else []
        }
        
        return stats

# Global instance
lead_search_service = LeadSearchService()
