"""DocuDB data loader for patient data"""
import pymongo
import pandas as pd
from typing import Dict, Any
import os
from src.config.settings import settings

class DocuDBLoader:
    def __init__(self):
        self.client = None
        self.db = None
        
    async def connect(self):
        """Connect to DocuDB"""
        connection_string = os.getenv('DOCUDB_CONNECTION_STRING')
        self.client = pymongo.MongoClient(connection_string, ssl=True)
        self.db = self.client[os.getenv('DOCUDB_DATABASE', 'patient_db')]
        
    async def load_patients(self, query_filters: Dict[str, Any]) -> pd.DataFrame:
        """Load patients from DocuDB based on query filters"""
        collection = self.db['patients']
        
        # Build MongoDB query from filters
        mongo_query = self._build_mongo_query(query_filters)
        
        # Execute query
        cursor = collection.find(mongo_query)
        patients = list(cursor)
        
        # Convert to DataFrame
        return pd.DataFrame(patients)
    
    def _build_mongo_query(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert API filters to MongoDB query"""
        query = {}
        
        if filters.get('condition'):
            query['condition'] = filters['condition']
            
        if filters.get('age_min') or filters.get('age_max'):
            age_query = {}
            if filters.get('age_min'):
                age_query['$gte'] = filters['age_min']
            if filters.get('age_max'):
                age_query['$lte'] = filters['age_max']
            query['age'] = age_query
            
        if filters.get('gender'):
            query['gender'] = filters['gender']
            
        return query