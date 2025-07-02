from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import json
import uuid
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return {
        'service': 'SCB Banking Bulk API Platform',
        'version': '1.0.0',
        'status': 'live',
        'message': 'Replacing file transfers with real-time APIs',
        'demo_endpoints': {
            'health': '/health',
            'teams': '/api/v1/teams',
            'legacy_cobol': '/api/v1/legacy/CUSTBAL',
            'bulk_extract': '/api/v1/bulk/extract'
        }
    }

@app.route('/health')
def health():
    return {'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()}

@app.route('/api/v1/teams')
def teams():
    return {
        'teams': {
            'risk-team': {'data_sources': ['risk_scores', 'fraud_alerts']},
            'operations-team': {'data_sources': ['transactions', 'accounts']},
            'compliance-team': {'data_sources': ['audit_logs', 'reports']},
            'analytics-team': {'data_sources': ['insights', 'metrics']}
        },
        'total_teams': 4
    }

@app.route('/api/v1/legacy/<program>', methods=['POST'])
def legacy(program):
    data = request.json or {}
    if program == 'CUSTBAL':
        result = {
            'account': data.get('account', '12345'),
            'balance': 47500.00,
            'currency': 'USD',
            'status': 'ACTIVE'
        }
    else:
        result = {'program': program, 'status': 'SUCCESS'}
    
    return {
        'job_id': str(uuid.uuid4()),
        'program': program,
        'result': result,
        'timestamp': datetime.utcnow().isoformat()
    }

@app.route('/api/v1/bulk/extract', methods=['POST'])
def bulk_extract():
    data = request.json or {}
    return {
        'job_id': str(uuid.uuid4()),
        'status': 'completed',
        'source': data.get('source', 'demo'),
        'records': 50,
        'message': 'Bulk extraction completed - file transfers eliminated!'
    }

import pandas as pd
import io
import base64
from flask import send_file

# NEW: CSV Upload Endpoint (System A calls this)
@app.route('/api/v1/csv/upload', methods=['POST'])
def csv_upload():
    """System A uploads CSV file, gets processed immediately"""
    
    # Get CSV file from request
    if 'file' not in request.files:
        return {'error': 'No CSV file provided'}, 400
    
    csv_file = request.files['file']
    if csv_file.filename == '':
        return {'error': 'No file selected'}, 400
    
    try:
        # Read CSV content
        csv_content = csv_file.read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Store processed data (in memory for immediate processing)
        processed_data = {
            'job_id': job_id,
            'filename': csv_file.filename,
            'records': df.to_dict('records'),
            'total_rows': len(df),
            'columns': list(df.columns),
            'uploaded_at': datetime.utcnow().isoformat(),
            'status': 'ready'
        }
        
        # In production, you'd send this to Kafka
        # For POC, store in memory (or Redis)
        store_csv_data(job_id, processed_data)
        
        return {
            'job_id': job_id,
            'status': 'processed',
            'message': 'CSV uploaded and processed successfully',
            'total_records': len(df),
            'columns': list(df.columns),
            'api_url': f'/api/v1/csv/download/{job_id}',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {'error': f'CSV processing failed: {str(e)}'}, 500

# NEW: CSV Download Endpoint (System B calls this)
@app.route('/api/v1/csv/download/<job_id>', methods=['GET'])
def csv_download(job_id):
    """System B downloads processed CSV data"""
    
    # Get processed data
    data = get_csv_data(job_id)
    if not data:
        return {'error': 'Job not found or expired'}, 404
    
    # Convert back to CSV format
    df = pd.DataFrame(data['records'])
    
    # Create CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False)
    csv_content = output.getvalue()
    
    # Return as downloadable file
    response = make_response(csv_content)
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename=processed_{data["filename"]}'
    
    return response

# NEW: API Data Access (Alternative to CSV download)
@app.route('/api/v1/csv/data/<job_id>', methods=['GET'])
def csv_data_api(job_id):
    """System B gets data as JSON API response (no CSV file)"""
    
    data = get_csv_data(job_id)
    if not data:
        return {'error': 'Job not found or expired'}, 404
    
    # Apply filters if provided
    filters = request.args.to_dict()
    records = data['records']
    
    if filters:
        df = pd.DataFrame(records)
        # Apply filters (example: ?region=APAC&balance=>10000)
        for key, value in filters.items():
            if key in df.columns:
                if value.startswith('>'):
                    df = df[df[key] > float(value[1:])]
                elif value.startswith('<'):
                    df = df[df[key] < float(value[1:])]
                else:
                    df = df[df[key] == value]
        records = df.to_dict('records')
    
    return {
        'job_id': job_id,
        'status': 'success',
        'total_records': len(records),
        'data': records,
        'metadata': {
            'original_filename': data['filename'],
            'uploaded_at': data['uploaded_at'],
            'columns': data['columns']
        }
    }

# Helper functions for data storage (in-memory for POC)
csv_storage = {}

def store_csv_data(job_id, data):
    csv_storage[job_id] = data

def get_csv_data(job_id):
    return csv_storage.get(job_id)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
