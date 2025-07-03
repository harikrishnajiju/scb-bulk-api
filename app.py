from flask import Flask, request, jsonify, send_file, make_response
from flask_cors import CORS
import json
import uuid
import os
import threading
import time
from datetime import datetime
import pandas as pd
import io
import redis

# Use confluent-kafka instead of kafka-python (more reliable)
try:
    from confluent_kafka import Producer, Consumer, KafkaError
    KAFKA_AVAILABLE = True
    print("✅ Confluent Kafka library loaded successfully")
except ImportError as e:
    print(f"⚠️  Kafka library not available: {e}")
    KAFKA_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

print(f"🚀 Starting Banking Bulk API Platform")
print(f"📡 Kafka: {KAFKA_SERVERS} (Available: {KAFKA_AVAILABLE})")
print(f"⚡ Redis: {REDIS_URL}")

# Initialize Kafka Producer
def init_kafka_producer():
    if not KAFKA_AVAILABLE:
        print("⚠️  Kafka not available, using fallback mode")
        return None
        
    retries = 5
    for i in range(retries):
        try:
            producer = Producer({
                'bootstrap.servers': KAFKA_SERVERS,
                'client.id': 'banking-api-producer',
                'acks': 'all',
                'retries': 3,
                'batch.size': 16384,
                'linger.ms': 10
            })
            print("✅ Kafka producer connected")
            return producer
        except Exception as e:
            print(f"❌ Kafka connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(2)
    
    print("❌ Failed to connect to Kafka, using fallback mode")
    return None

# Initialize Redis
def init_redis_client():
    retries = 3
    for i in range(retries):
        try:
            client = redis.from_url(REDIS_URL)
            client.ping()
            print("✅ Redis connected")
            return client
        except Exception as e:
            print(f"❌ Redis connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(1)
    
    print("❌ Failed to connect to Redis, using in-memory fallback")
    return None

# Initialize connections
kafka_producer = init_kafka_producer()
redis_client = init_redis_client()

# Kafka Topics
TOPICS = {
    'CSV_UPLOAD': 'csv-upload-requests',
    'CSV_PROCESSED': 'csv-processed-data', 
    'CSV_DOWNLOAD': 'csv-download-requests',
    'AUDIT_LOG': 'system-audit-logs',
    'LEGACY_REQUESTS': 'legacy-system-requests'
}

# Fallback storage
csv_storage = {}
audit_logs = []

# Helper function to send to Kafka
def send_to_kafka(topic, message, key=None):
    """Send message to Kafka with error handling"""
    if not kafka_producer:
        return False
    
    try:
        # Convert message to JSON string
        message_json = json.dumps(message)
        
        # Send message
        kafka_producer.produce(
            topic=topic,
            key=key,
            value=message_json,
            callback=lambda err, msg: print(f"✅ Kafka delivery: {msg.topic()}" if not err else f"❌ Kafka error: {err}")
        )
        
        # Flush to ensure delivery
        kafka_producer.flush(timeout=1)
        return True
        
    except Exception as e:
        print(f"❌ Failed to send to Kafka: {e}")
        return False

@app.route('/')
def index():
    return {
        'service': 'SCB Banking Bulk API Platform',
        'version': '2.0.0',
        'status': 'live',
        'architecture': 'Kafka-enabled' if kafka_producer else 'Fallback-mode',
        'message': 'Replacing file transfers with real-time APIs',
        'kafka_enabled': kafka_producer is not None,
        'redis_enabled': redis_client is not None,
        'demo_endpoints': {
            'health': '/health',
            'teams': '/api/v1/teams',
            'legacy_cobol': '/api/v1/legacy/CUSTBAL',
            'bulk_extract': '/api/v1/bulk/extract',
            'csv_upload': '/api/v1/csv/upload',
            'csv_download': '/api/v1/csv/download/{job_id}',
            'audit_logs': '/api/v1/audit/logs'
        }
    }

@app.route('/health')
def health():
    status = 'healthy'
    services = {}
    
    # Check Kafka
    if kafka_producer:
        try:
            # Test Kafka by sending a health check message
            test_sent = send_to_kafka('health-check', {
                'timestamp': datetime.utcnow().isoformat(),
                'service': 'banking-api'
            })
            services['kafka'] = 'connected' if test_sent else 'error'
        except Exception as e:
            services['kafka'] = f'error: {str(e)}'
            status = 'degraded'
    else:
        services['kafka'] = 'fallback_mode'
        status = 'degraded'
    
    # Check Redis
    if redis_client:
        try:
            redis_client.ping()
            services['redis'] = 'connected'
        except Exception as e:
            services['redis'] = f'error: {str(e)}'
            status = 'degraded'
    else:
        services['redis'] = 'fallback_mode'
        status = 'degraded'
    
    return {
        'status': status,
        'timestamp': datetime.utcnow().isoformat(),
        'services': services,
        'kafka_topics': list(TOPICS.values()) if kafka_producer else 'disabled',
        'storage_mode': 'kafka+redis' if kafka_producer and redis_client else 'memory'
    }

@app.route('/api/v1/teams')
def teams():
    return {
        'teams': {
            'risk-team': {'data_sources': ['risk_scores', 'fraud_alerts']},
            'operations-team': {'data_sources': ['transactions', 'accounts']},
            'compliance-team': {'data_sources': ['audit_logs', 'reports']},
            'analytics-team': {'data_sources': ['insights', 'metrics']}
        },
        'total_teams': 4,
        'kafka_enabled': kafka_producer is not None,
        'redis_enabled': redis_client is not None
    }

@app.route('/api/v1/legacy/<program>', methods=['POST'])
def legacy(program):
    data = request.json or {}
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    job_id = str(uuid.uuid4())
    
    # Legacy system simulation
    if program == 'CUSTBAL':
        result = {
            'account': data.get('account', '12345'),
            'balance': 47500.00,
            'available_balance': 45000.00,
            'currency': 'USD',
            'account_type': 'SAVINGS',
            'last_transaction_date': '2025-06-30',
            'status': 'ACTIVE'
        }
    elif program == 'TXNHIST':
        result = {
            'account': data.get('account', '12345'),
            'transactions': [
                {'date': '2025-06-30T14:30:00Z', 'amount': -150.00, 'description': 'ATM Withdrawal'},
                {'date': '2025-06-30T09:15:00Z', 'amount': 2800.00, 'description': 'Salary Deposit'},
                {'date': '2025-06-29T16:45:00Z', 'amount': -75.50, 'description': 'Online Purchase'}
            ]
        }
    elif program == 'RISKSCORE':
        result = {
            'customer_id': data.get('customer_id', '12345'),
            'risk_score': 7.2,
            'risk_category': 'MEDIUM',
            'factors': ['Multiple international transfers', 'High transaction velocity']
        }
    else:
        result = {'program': program, 'status': 'SUCCESS'}
    
    # Create audit data
    audit_data = {
        'job_id': job_id,
        'event_type': 'legacy_system_call',
        'program': program,
        'requesting_team': requesting_team,
        'timestamp': datetime.utcnow().isoformat(),
        'request_data': data
    }
    
    # Log to Kafka or fallback
    kafka_logged = False
    if kafka_producer:
        kafka_logged = send_to_kafka(TOPICS['LEGACY_REQUESTS'], audit_data, job_id)
        send_to_kafka(TOPICS['AUDIT_LOG'], audit_data, job_id)
    
    if not kafka_logged:
        audit_logs.append(audit_data)
    
    return {
        'job_id': job_id,
        'program': program,
        'requesting_team': requesting_team,
        'result': result,
        'timestamp': datetime.utcnow().isoformat(),
        'kafka_logged': kafka_logged,
        'storage_backend': 'kafka' if kafka_logged else 'memory'
    }

@app.route('/api/v1/bulk/extract', methods=['POST'])
def bulk_extract():
    data = request.json or {}
    job_id = str(uuid.uuid4())
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Create audit data
    audit_data = {
        'job_id': job_id,
        'event_type': 'bulk_extract_request',
        'requesting_team': requesting_team,
        'source': data.get('source', 'demo'),
        'filters': data.get('filters', {}),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Log to Kafka or fallback
    kafka_logged = False
    if kafka_producer:
        kafka_logged = send_to_kafka(TOPICS['AUDIT_LOG'], audit_data, job_id)
    
    if not kafka_logged:
        audit_logs.append(audit_data)
    
    return {
        'job_id': job_id,
        'status': 'completed',
        'source': data.get('source', 'demo'),
        'records': 50,
        'message': 'Bulk extraction completed - file transfers eliminated!',
        'kafka_logged': kafka_logged,
        'storage_backend': 'kafka' if kafka_logged else 'memory'
    }

@app.route('/api/v1/csv/upload', methods=['POST'])
def csv_upload():
    """System A uploads CSV file, processed with Kafka for durability"""
    
    if 'file' not in request.files:
        return {'error': 'No CSV file provided'}, 400
    
    csv_file = request.files['file']
    if csv_file.filename == '':
        return {'error': 'No file selected'}, 400
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    job_id = str(uuid.uuid4())
    
    try:
        # Read and process CSV
        csv_content = csv_file.read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        
        processed_data = {
            'job_id': job_id,
            'filename': csv_file.filename,
            'records': df.to_dict('records'),
            'total_rows': len(df),
            'columns': list(df.columns),
            'uploaded_at': datetime.utcnow().isoformat(),
            'uploaded_by': requesting_team,
            'status': 'ready'
        }
        
        # Store in Kafka and Redis
        kafka_logged = False
        redis_cached = False
        
        if kafka_producer:
            # Send upload event
            upload_event = {
                'job_id': job_id,
                'event_type': 'csv_upload',
                'team': requesting_team,
                'filename': csv_file.filename,
                'record_count': len(df),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            kafka_logged = send_to_kafka(TOPICS['CSV_UPLOAD'], upload_event, job_id)
            
            # Store processed data
            if kafka_logged:
                kafka_logged = send_to_kafka(TOPICS['CSV_PROCESSED'], {
                    'job_id': job_id,
                    'data': processed_data
                }, job_id)
        
        # Store metadata in Redis
        if redis_client:
            try:
                metadata = {
                    'filename': csv_file.filename,
                    'record_count': len(df),
                    'uploaded_by': requesting_team,
                    'timestamp': datetime.utcnow().isoformat(),
                    'columns': list(df.columns)
                }
                redis_client.setex(f'csv_meta:{job_id}', 3600, json.dumps(metadata))
                redis_cached = True
            except Exception as e:
                print(f"❌ Redis caching failed: {e}")
        
        # Fallback to memory if Kafka failed
        if not kafka_logged:
            csv_storage[job_id] = processed_data
        
        storage_backend = []
        if kafka_logged:
            storage_backend.append('kafka')
        if redis_cached:
            storage_backend.append('redis')
        if not kafka_logged:
            storage_backend.append('memory')
        
        return {
            'job_id': job_id,
            'status': 'processed',
            'message': 'CSV uploaded and processed successfully',
            'total_records': len(df),
            'columns': list(df.columns),
            'api_url': f'/api/v1/csv/download/{job_id}',
            'timestamp': datetime.utcnow().isoformat(),
            'kafka_enabled': kafka_producer is not None,
            'kafka_logged': kafka_logged,
            'redis_cached': redis_cached,
            'storage_backend': '+'.join(storage_backend)
        }
        
    except Exception as e:
        return {'error': f'CSV processing failed: {str(e)}'}, 500

@app.route('/api/v1/csv/download/<job_id>', methods=['GET'])
def csv_download(job_id):
    """System B downloads processed CSV data"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Log download request
    download_event = {
        'job_id': job_id,
        'event_type': 'csv_download_request',
        'requesting_team': requesting_team,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if kafka_producer:
        send_to_kafka(TOPICS['CSV_DOWNLOAD'], download_event, job_id)
    
    # Get data from storage
    data = None
    
    # Try Redis first (fastest)
    if redis_client:
        try:
            cached_data = redis_client.get(f'csv_data:{job_id}')
            if cached_data:
                data = json.loads(cached_data)
                print(f"✅ Data retrieved from Redis cache")
        except Exception as e:
            print(f"❌ Redis retrieval failed: {e}")
    
    # Fallback to memory storage
    if not data:
        data = csv_storage.get(job_id)
        if data:
            print(f"✅ Data retrieved from memory storage")
    
    if not data:
        return {'error': 'Job not found or expired'}, 404
    
    try:
        # Convert to CSV
        df = pd.DataFrame(data['records'])
        output = io.StringIO()
        df.to_csv(output, index=False)
        csv_content = output.getvalue()
        
        # Return CSV file
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=processed_{data["filename"]}'
        
        return response
        
    except Exception as e:
        return {'error': f'CSV generation failed: {str(e)}'}, 500

@app.route('/api/v1/csv/data/<job_id>', methods=['GET'])
def csv_data_api(job_id):
    """System B gets data as JSON API response"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Get data from storage (same logic as download)
    data = None
    storage_source = 'unknown'
    
    # Try Redis first
    if redis_client:
        try:
            cached_data = redis_client.get(f'csv_data:{job_id}')
            if cached_data:
                data = json.loads(cached_data)
                storage_source = 'redis'
        except Exception as e:
            print(f"❌ Redis retrieval failed: {e}")
    
    # Fallback to memory
    if not data:
        data = csv_storage.get(job_id)
        storage_source = 'memory'
    
    if not data:
        return {'error': 'Job not found or expired'}, 404
    
    # Apply filters
    filters = request.args.to_dict()
    records = data['records']
    
    if filters:
        df = pd.DataFrame(records)
        for key, value in filters.items():
            if key in df.columns:
                try:
                    if value.startswith('>'):
                        df = df[df[key] > float(value[1:])]
                    elif value.startswith('<'):
                        df = df[df[key] < float(value[1:])]
                    else:
                        df = df[df[key] == value]
                except Exception as e:
                    print(f"❌ Filter error for {key}={value}: {e}")
        records = df.to_dict('records')
    
    return {
        'job_id': job_id,
        'status': 'success',
        'total_records': len(records),
        'data': records,
        'metadata': {
            'original_filename': data['filename'],
            'uploaded_at': data['uploaded_at'],
            'uploaded_by': data.get('uploaded_by', 'unknown'),
            'columns': data['columns']
        },
        'storage_backend': storage_source,
        'requesting_team': requesting_team,
        'kafka_enabled': kafka_producer is not None,
        'redis_enabled': redis_client is not None
    }

@app.route('/api/v1/audit/logs', methods=['GET'])
def audit_logs_api():
    """Get audit logs"""
    
    limit = int(request.args.get('limit', 10))
    
    # For now, return in-memory logs (in production, implement Kafka consumer)
    logs = audit_logs[-limit:] if audit_logs else []
    
    return {
        'audit_logs': logs,
        'total_logs': len(logs),
        'storage_backend': 'kafka' if kafka_producer else 'memory',
        'timestamp': datetime.utcnow().isoformat(),
        'kafka_enabled': kafka_producer is not None
    }

# Store processed CSV data in Redis for faster access
def cache_csv_data_in_redis(job_id, data):
    """Cache CSV data in Redis for faster retrieval"""
    if redis_client:
        try:
            redis_client.setex(f'csv_data:{job_id}', 3600, json.dumps(data))
            return True
        except Exception as e:
            print(f"❌ Failed to cache in Redis: {e}")
    return False

if __name__ == '__main__':
    print("🚀 Banking Bulk API Platform Starting...")
    print("📋 Features:")
    print(f"   • Kafka messaging: {'✅ Enabled' if kafka_producer else '❌ Fallback mode'}")
    print(f"   • Redis caching: {'✅ Enabled' if redis_client else '❌ Fallback mode'}")
    print("   • Real-time CSV processing")
    print("   • Complete audit trails")
    print("   • Legacy system integration")
    
    port = int(os.environ.get('PORT', 5000))
    print(f"🌐 API Server starting on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)