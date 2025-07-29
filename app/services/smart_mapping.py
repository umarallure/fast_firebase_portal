"""
Smart Mapping Strategy for GoHighLevel Migration
Handles intelligent mapping of pipelines, stages, and custom fields
"""

import logging
from typing import Dict, List, Any, Tuple, Optional
from difflib import SequenceMatcher
import re

logger = logging.getLogger(__name__)

class SmartMappingStrategy:
    """Intelligent mapping strategy for migration data"""
    
    def __init__(self):
        self.similarity_threshold = 0.8  # 80% similarity for auto-mapping
        self.common_stage_mappings = {
            # Common stage name variations
            "lead": ["lead", "new lead", "incoming lead", "fresh lead"],
            "qualified": ["qualified", "qualified lead", "qualified prospect", "sql"],
            "proposal": ["proposal", "proposal sent", "quote sent", "estimate"],
            "negotiation": ["negotiation", "negotiate", "discussing", "in negotiation"],
            "closed_won": ["closed won", "won", "closed", "deal won", "successful"],
            "closed_lost": ["closed lost", "lost", "rejected", "declined", "failed"],
            "follow_up": ["follow up", "follow-up", "pending", "waiting"],
            "demo": ["demo", "demonstration", "presentation", "meeting scheduled"]
        }
        
        self.common_field_mappings = {
            # Common custom field name variations
            "industry": ["industry", "business type", "sector", "vertical"],
            "company_size": ["company size", "employees", "team size", "staff count"],
            "budget": ["budget", "deal value", "project budget", "investment"],
            "source": ["source", "lead source", "origin", "referral source"],
            "priority": ["priority", "importance", "urgency", "level"],
            "notes": ["notes", "comments", "description", "details"],
            "website": ["website", "url", "domain", "web address"]
        }

    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two text strings"""
        if not text1 or not text2:
            return 0.0
        
        # Normalize text for comparison
        text1_norm = self._normalize_text(text1)
        text2_norm = self._normalize_text(text2)
        
        # Calculate similarity using SequenceMatcher
        similarity = SequenceMatcher(None, text1_norm, text2_norm).ratio()
        
        return similarity

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison"""
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def find_best_stage_match(self, child_stage_name: str, master_stages: List[Dict[str, Any]]) -> Optional[str]:
        """Find the best matching stage in master account"""
        if not child_stage_name or not master_stages:
            return None
        
        child_stage_norm = self._normalize_text(child_stage_name)
        best_match = None
        best_similarity = 0.0
        
        # First, try exact matches with common mappings
        for standard_name, variations in self.common_stage_mappings.items():
            if child_stage_norm in [self._normalize_text(v) for v in variations]:
                # Look for this standard name in master stages
                for master_stage in master_stages:
                    master_name_norm = self._normalize_text(master_stage["name"])
                    if master_name_norm in [self._normalize_text(v) for v in variations]:
                        logger.info(f"Found common mapping for stage '{child_stage_name}' -> '{master_stage['name']}'")
                        return master_stage["id"]
        
        # If no common mapping found, use similarity matching
        for master_stage in master_stages:
            similarity = self.calculate_similarity(child_stage_name, master_stage["name"])
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = master_stage
        
        # Return best match if similarity is above threshold
        if best_match and best_similarity >= self.similarity_threshold:
            logger.info(f"Found similarity match for stage '{child_stage_name}' -> '{best_match['name']}' (similarity: {best_similarity:.2f})")
            return best_match["id"]
        
        logger.warning(f"No suitable match found for stage '{child_stage_name}' (best similarity: {best_similarity:.2f})")
        return None

    def find_best_field_match(self, child_field_name: str, master_fields: List[Dict[str, Any]]) -> Optional[str]:
        """Find the best matching custom field in master account"""
        if not child_field_name or not master_fields:
            return None
        
        child_field_norm = self._normalize_text(child_field_name)
        best_match = None
        best_similarity = 0.0
        
        # First, try exact matches with common mappings
        for standard_name, variations in self.common_field_mappings.items():
            if child_field_norm in [self._normalize_text(v) for v in variations]:
                # Look for this standard name in master fields
                for master_field in master_fields:
                    master_name_norm = self._normalize_text(master_field["name"])
                    if master_name_norm in [self._normalize_text(v) for v in variations]:
                        logger.info(f"Found common mapping for field '{child_field_name}' -> '{master_field['name']}'")
                        return master_field["id"]
        
        # If no common mapping found, use similarity matching
        for master_field in master_fields:
            similarity = self.calculate_similarity(child_field_name, master_field["name"])
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = master_field
        
        # Return best match if similarity is above threshold
        if best_match and best_similarity >= self.similarity_threshold:
            logger.info(f"Found similarity match for field '{child_field_name}' -> '{best_match['name']}' (similarity: {best_similarity:.2f})")
            return best_match["id"]
        
        logger.warning(f"No suitable match found for field '{child_field_name}' (best similarity: {best_similarity:.2f})")
        return None

    def create_pipeline_mapping_strategy(self, child_pipelines: List[Dict[str, Any]], 
                                       master_pipelines: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Create a comprehensive mapping strategy for pipelines and stages"""
        mapping_strategy = {
            "pipeline_mappings": {},
            "stage_mappings": {},
            "unmapped_pipelines": [],
            "unmapped_stages": [],
            "recommendations": []
        }
        
        master_pipelines_by_name = {self._normalize_text(p["name"]): p for p in master_pipelines}
        
        for child_pipeline in child_pipelines:
            child_pipeline_id = child_pipeline["id"]
            child_pipeline_name = child_pipeline["name"]
            child_stages = child_pipeline.get("stages", [])
            
            # Try to find matching master pipeline
            child_name_norm = self._normalize_text(child_pipeline_name)
            master_pipeline = None
            
            # First try exact name match
            if child_name_norm in master_pipelines_by_name:
                master_pipeline = master_pipelines_by_name[child_name_norm]
            else:
                # Try similarity matching
                best_similarity = 0.0
                for master_pipe in master_pipelines:
                    similarity = self.calculate_similarity(child_pipeline_name, master_pipe["name"])
                    if similarity > best_similarity and similarity >= self.similarity_threshold:
                        best_similarity = similarity
                        master_pipeline = master_pipe
            
            if master_pipeline:
                # Map the pipeline
                mapping_strategy["pipeline_mappings"][child_pipeline_id] = master_pipeline["id"]
                logger.info(f"Mapped pipeline '{child_pipeline_name}' -> '{master_pipeline['name']}'")
                
                # Map stages within the pipeline
                master_stages = master_pipeline.get("stages", [])
                for child_stage in child_stages:
                    child_stage_id = child_stage["id"]
                    master_stage_id = self.find_best_stage_match(child_stage["name"], master_stages)
                    
                    if master_stage_id:
                        mapping_strategy["stage_mappings"][child_stage_id] = master_stage_id
                    else:
                        mapping_strategy["unmapped_stages"].append({
                            "child_stage": child_stage,
                            "pipeline_name": child_pipeline_name,
                            "master_pipeline_id": master_pipeline["id"]
                        })
                        mapping_strategy["recommendations"].append(
                            f"Create stage '{child_stage['name']}' in pipeline '{master_pipeline['name']}'"
                        )
            else:
                # Pipeline not found
                mapping_strategy["unmapped_pipelines"].append(child_pipeline)
                mapping_strategy["recommendations"].append(
                    f"Create pipeline '{child_pipeline_name}' with {len(child_stages)} stages"
                )
        
        return mapping_strategy

    def create_custom_field_mapping_strategy(self, child_fields: List[Dict[str, Any]], 
                                           master_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a comprehensive mapping strategy for custom fields"""
        mapping_strategy = {
            "field_mappings": {},
            "unmapped_fields": [],
            "type_mismatches": [],
            "recommendations": []
        }
        
        for child_field in child_fields:
            child_field_id = child_field["id"]
            child_field_name = child_field["name"]
            child_field_type = child_field.get("type", "TEXT")
            
            master_field_id = self.find_best_field_match(child_field_name, master_fields)
            
            if master_field_id:
                # Check if field types match
                master_field = next((f for f in master_fields if f["id"] == master_field_id), None)
                if master_field:
                    master_field_type = master_field.get("type", "TEXT")
                    if child_field_type == master_field_type:
                        mapping_strategy["field_mappings"][child_field_id] = master_field_id
                        logger.info(f"Mapped field '{child_field_name}' -> '{master_field['name']}'")
                    else:
                        mapping_strategy["type_mismatches"].append({
                            "child_field": child_field,
                            "master_field": master_field,
                            "child_type": child_field_type,
                            "master_type": master_field_type
                        })
                        mapping_strategy["recommendations"].append(
                            f"Type mismatch for field '{child_field_name}': {child_field_type} vs {master_field_type}"
                        )
            else:
                # Field not found, needs to be created
                mapping_strategy["unmapped_fields"].append(child_field)
                mapping_strategy["recommendations"].append(
                    f"Create custom field '{child_field_name}' of type '{child_field_type}'"
                )
        
        return mapping_strategy

    def optimize_contact_processing_order(self, contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Optimize the order of contact processing for better success rates"""
        # Prioritize contacts with:
        # 1. Email addresses (easier to dedupe)
        # 2. Phone numbers
        # 3. Complete information
        
        def contact_priority(contact):
            score = 0
            
            # Email gets highest priority
            if contact.get("email"):
                score += 100
            
            # Phone number gets second priority  
            if contact.get("phone"):
                score += 50
            
            # Complete name information
            if contact.get("firstName") and contact.get("lastName"):
                score += 20
            
            # Address information
            if contact.get("address1") and contact.get("city"):
                score += 10
            
            # Custom fields data
            if contact.get("customFields"):
                score += len(contact["customFields"])
            
            return score
        
        # Sort contacts by priority (highest first)
        sorted_contacts = sorted(contacts, key=contact_priority, reverse=True)
        
        logger.info(f"Optimized contact processing order. Top priority contacts have emails and complete data.")
        return sorted_contacts

    def generate_migration_report(self, pipeline_strategy: Dict[str, Any], 
                                field_strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive migration report"""
        report = {
            "summary": {
                "pipelines_mapped": len(pipeline_strategy["pipeline_mappings"]),
                "pipelines_unmapped": len(pipeline_strategy["unmapped_pipelines"]),
                "stages_mapped": len(pipeline_strategy["stage_mappings"]),
                "stages_unmapped": len(pipeline_strategy["unmapped_stages"]),
                "fields_mapped": len(field_strategy["field_mappings"]),
                "fields_unmapped": len(field_strategy["unmapped_fields"]),
                "type_mismatches": len(field_strategy["type_mismatches"])
            },
            "pipeline_details": pipeline_strategy,
            "field_details": field_strategy,
            "recommendations": {
                "pipeline_recommendations": pipeline_strategy["recommendations"],
                "field_recommendations": field_strategy["recommendations"],
                "migration_readiness": self._assess_migration_readiness(pipeline_strategy, field_strategy)
            }
        }
        
        return report

    def _assess_migration_readiness(self, pipeline_strategy: Dict[str, Any], 
                                  field_strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Assess how ready the accounts are for migration"""
        total_pipelines = len(pipeline_strategy["pipeline_mappings"]) + len(pipeline_strategy["unmapped_pipelines"])
        total_fields = len(field_strategy["field_mappings"]) + len(field_strategy["unmapped_fields"])
        
        pipeline_readiness = len(pipeline_strategy["pipeline_mappings"]) / max(total_pipelines, 1) * 100
        field_readiness = len(field_strategy["field_mappings"]) / max(total_fields, 1) * 100
        
        overall_readiness = (pipeline_readiness + field_readiness) / 2
        
        readiness_assessment = {
            "pipeline_readiness_percent": round(pipeline_readiness, 2),
            "field_readiness_percent": round(field_readiness, 2),
            "overall_readiness_percent": round(overall_readiness, 2),
            "readiness_level": "high" if overall_readiness >= 80 else "medium" if overall_readiness >= 60 else "low",
            "can_proceed": overall_readiness >= 50,  # Minimum 50% readiness to proceed
            "warnings": []
        }
        
        if pipeline_readiness < 50:
            readiness_assessment["warnings"].append("Many pipelines are not mapped - opportunities may fail to migrate")
        
        if field_readiness < 50:
            readiness_assessment["warnings"].append("Many custom fields are not mapped - data may be lost")
        
        if len(field_strategy["type_mismatches"]) > 0:
            readiness_assessment["warnings"].append("Field type mismatches detected - manual review required")
        
        return readiness_assessment
