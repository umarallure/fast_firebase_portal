"""
GoHighLevel V1 API Documentation Service
Provides API documentation and examples for migration endpoints
"""

from typing import Dict, Any, List

class GHLAPIDocumentation:
    """Documentation for GoHighLevel V1 API endpoints used in migration"""
    
    @staticmethod
    def get_custom_fields_docs() -> Dict[str, Any]:
        """Documentation for custom fields endpoints"""
        return {
            "title": "Custom Fields API",
            "description": "Manage custom fields for contacts and opportunities",
            "endpoints": {
                "get_contact_custom_fields": {
                    "method": "GET",
                    "url": "/v1/custom-fields",
                    "description": "Fetch all custom fields for contacts",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "response_example": {
                        "customFields": [
                            {
                                "id": "field_id_123",
                                "name": "Industry",
                                "type": "TEXT",
                                "options": []
                            }
                        ]
                    }
                },
                "get_opportunity_custom_fields": {
                    "method": "GET",
                    "url": "/v1/custom-fields/opportunity",
                    "description": "Fetch all custom fields for opportunities",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "response_example": {
                        "customFields": [
                            {
                                "id": "opp_field_id_123",
                                "name": "Deal Source",
                                "type": "SELECT",
                                "options": ["Website", "Referral", "Cold Call"]
                            }
                        ]
                    }
                },
                "create_contact_custom_field": {
                    "method": "POST",
                    "url": "/v1/custom-fields",
                    "description": "Create a new custom field for contacts",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "request_example": {
                        "name": "Industry",
                        "type": "TEXT"
                    },
                    "response_example": {
                        "customField": {
                            "id": "new_field_id_123",
                            "name": "Industry",
                            "type": "TEXT"
                        }
                    }
                },
                "create_opportunity_custom_field": {
                    "method": "POST",
                    "url": "/v1/custom-fields/opportunity",
                    "description": "Create a new custom field for opportunities",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "request_example": {
                        "name": "Deal Source",
                        "type": "SELECT",
                        "options": ["Website", "Referral", "Cold Call"]
                    },
                    "response_example": {
                        "customField": {
                            "id": "new_opp_field_id_123",
                            "name": "Deal Source",
                            "type": "SELECT",
                            "options": ["Website", "Referral", "Cold Call"]
                        }
                    }
                }
            },
            "field_types": [
                "TEXT",
                "NUMERICAL", 
                "PHONE",
                "EMAIL",
                "SELECT",
                "RADIO",
                "CHECKBOX",
                "DATE",
                "URL"
            ]
        }
    
    @staticmethod
    def get_contacts_docs() -> Dict[str, Any]:
        """Documentation for contacts endpoints"""
        return {
            "title": "Contacts API",
            "description": "Manage contacts and their data",
            "endpoints": {
                "get_contacts": {
                    "method": "GET",
                    "url": "/v1/contacts",
                    "description": "Fetch contacts with pagination",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "query_params": {
                        "page": "Page number (default: 1)",
                        "limit": "Number of contacts per page (default: 100, max: 100)",
                        "email": "Filter by email address",
                        "phone": "Filter by phone number"
                    },
                    "response_example": {
                        "contacts": [
                            {
                                "id": "contact_id_123",
                                "firstName": "John",
                                "lastName": "Doe",
                                "email": "john@example.com",
                                "phone": "+1234567890",
                                "address1": "123 Main St",
                                "city": "Anytown",
                                "state": "CA",
                                "postalCode": "12345",
                                "country": "US",
                                "customFields": [
                                    {
                                        "fieldId": "field_id_123",
                                        "value": "Technology"
                                    }
                                ]
                            }
                        ]
                    }
                },
                "create_contact": {
                    "method": "POST",
                    "url": "/v1/contacts",
                    "description": "Create a new contact",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "request_example": {
                        "firstName": "John",
                        "lastName": "Doe",
                        "email": "john@example.com",
                        "phone": "+1234567890",
                        "address1": "123 Main St",
                        "city": "Anytown",
                        "state": "CA",
                        "postalCode": "12345",
                        "country": "US",
                        "customFields": [
                            {
                                "fieldId": "field_id_123",
                                "value": "Technology"
                            }
                        ]
                    },
                    "response_example": {
                        "contact": {
                            "id": "new_contact_id_123",
                            "firstName": "John",
                            "lastName": "Doe",
                            "email": "john@example.com"
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def get_opportunities_docs() -> Dict[str, Any]:
        """Documentation for opportunities endpoints"""
        return {
            "title": "Opportunities API",
            "description": "Manage opportunities and deals",
            "endpoints": {
                "get_opportunities": {
                    "method": "GET",
                    "url": "/v1/opportunities",
                    "description": "Fetch opportunities with pagination",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "query_params": {
                        "page": "Page number (default: 1)",
                        "limit": "Number of opportunities per page (default: 100, max: 100)"
                    },
                    "response_example": {
                        "opportunities": [
                            {
                                "id": "opp_id_123",
                                "name": "Website Redesign Project",
                                "contactId": "contact_id_123",
                                "pipelineId": "pipeline_id_123",
                                "pipelineStageId": "stage_id_123",
                                "status": "open",
                                "value": 5000.00,
                                "customFields": [
                                    {
                                        "fieldId": "opp_field_id_123",
                                        "value": "Website"
                                    }
                                ]
                            }
                        ]
                    }
                },
                "create_opportunity": {
                    "method": "POST",
                    "url": "/v1/opportunities",
                    "description": "Create a new opportunity",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "request_example": {
                        "name": "Website Redesign Project",
                        "contactId": "contact_id_123",
                        "pipelineId": "pipeline_id_123",
                        "stageId": "stage_id_123",
                        "status": "open",
                        "value": 5000.00,
                        "customFields": [
                            {
                                "fieldId": "opp_field_id_123",
                                "value": "Website"
                            }
                        ]
                    },
                    "response_example": {
                        "opportunity": {
                            "id": "new_opp_id_123",
                            "name": "Website Redesign Project",
                            "contactId": "contact_id_123",
                            "pipelineId": "pipeline_id_123",
                            "status": "open",
                            "value": 5000.00
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def get_pipelines_docs() -> Dict[str, Any]:
        """Documentation for pipelines endpoints"""
        return {
            "title": "Pipelines API",
            "description": "Manage sales pipelines and stages",
            "endpoints": {
                "get_pipelines": {
                    "method": "GET",
                    "url": "/v1/pipelines",
                    "description": "Fetch all pipelines with their stages",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "response_example": {
                        "pipelines": [
                            {
                                "id": "pipeline_id_123",
                                "name": "Sales Pipeline",
                                "stages": [
                                    {
                                        "id": "stage_id_123",
                                        "name": "Lead",
                                        "position": 1
                                    },
                                    {
                                        "id": "stage_id_124",
                                        "name": "Qualified",
                                        "position": 2
                                    }
                                ]
                            }
                        ]
                    }
                },
                "create_pipeline_stage": {
                    "method": "POST",
                    "url": "/v1/pipelines/{pipeline_id}/stages",
                    "description": "Create a new stage in a pipeline",
                    "headers": {
                        "Authorization": "Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    },
                    "request_example": {
                        "name": "Closed Won",
                        "position": 3
                    },
                    "response_example": {
                        "stage": {
                            "id": "new_stage_id_123",
                            "name": "Closed Won",
                            "position": 3
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def get_all_docs() -> Dict[str, Any]:
        """Get all API documentation"""
        return {
            "title": "GoHighLevel V1 API Migration Documentation",
            "base_url": "https://rest.gohighlevel.com",
            "authentication": {
                "type": "Bearer Token",
                "description": "Use Location API Key as Bearer token",
                "header": "Authorization: Bearer {API_KEY}"
            },
            "rate_limits": {
                "description": "Implement rate limiting with delays between requests",
                "recommended_delay": "0.1 seconds between requests",
                "error_code": 429
            },
            "pagination": {
                "description": "Most endpoints support pagination",
                "parameters": {
                    "page": "Page number (1-based)",
                    "limit": "Items per page (max 100)"
                }
            },
            "apis": {
                "custom_fields": GHLAPIDocumentation.get_custom_fields_docs(),
                "contacts": GHLAPIDocumentation.get_contacts_docs(),
                "opportunities": GHLAPIDocumentation.get_opportunities_docs(),
                "pipelines": GHLAPIDocumentation.get_pipelines_docs()
            }
        }

# API Documentation endpoint responses
def get_api_docs_for_endpoint(endpoint_name: str) -> Dict[str, Any]:
    """Get documentation for a specific endpoint"""
    docs_map = {
        "custom-fields": GHLAPIDocumentation.get_custom_fields_docs(),
        "contacts": GHLAPIDocumentation.get_contacts_docs(),
        "opportunities": GHLAPIDocumentation.get_opportunities_docs(),
        "pipelines": GHLAPIDocumentation.get_pipelines_docs(),
        "all": GHLAPIDocumentation.get_all_docs()
    }
    
    return docs_map.get(endpoint_name, {"error": "Documentation not found for endpoint"})
