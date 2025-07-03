from flask import Flask, request, jsonify, send_file, make_response
from flask_cors import CORS
import json
import uuid
import os
import threading
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import io
import redis

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

print(f"🚀 Starting Banking Bulk API Platform with Kafka")
print(f"📡 Kafka: {KAFKA_SERVERS}")
print(f"⚡ Redis: {REDIS_URL}")

# Initialize Kafka Producer
def init_kafka_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10
            )
            print("✅ Kafka producer connected")
            return producer
        except Exception as e:
            print(f"❌ Kafka connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Kafka")

# Initialize Redis
def init_redis_client():
    retries = 5
    for i in range(retries):
        try:
            client = redis.from_url(REDIS_URL)
            client.ping()
            print("✅ Redis connected")
            return client
        except Exception as e:
            print(f"❌ Redis connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Redis")

# Initialize connections
try:
    kafka_producer = init_kafka_producer()
    redis_client = init_redis_client()
except Exception as e:
    print(f"❌ Failed to initialize connections: {e}")
    # Fallback to in-memory storage for development
    kafka_producer = None
    redis_client = None
    print("⚠️  Running in fallback mode (in-memory storage)")

# Kafka Topics
TOPICS = {
    'CSV_UPLOAD': 'csv-upload-requests',
    'CSV_PROCESSED': 'csv-processed-data', 
    'CSV_DOWNLOAD': 'csv-download-requests',
    'AUDIT_LOG': 'system-audit-logs',
    'LEGACY_REQUESTS': 'legacy-system-requests'
}

# Fallback in-memory storage
csv_storage = {}
audit_logs = []

@app.route('/')
def index():
    return {
        'service': 'SCB Banking Bulk API Platform',
        'version': '2.0.0',
        'status': 'live',
        'architecture': 'Kafka-enabled',
        'message': 'Replacing file transfers with real-time APIs',
        'kafka_enabled': kafka_producer is not None,
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
    try:
        if kafka_producer:
            kafka_producer.send('health-check', {
                'timestamp': datetime.utcnow().isoformat(),
                'service': 'banking-api'
            })
            services['kafka'] = 'connected'
        else:
            services['kafka'] = 'fallback_mode'
    except Exception as e:
        services['kafka'] = f'error: {str(e)}'
        status = 'degraded'
    
    # Check Redis
    try:
        if redis_client:
            redis_client.ping()
            services['redis'] = 'connected'
        else:
            services['redis'] = 'fallback_mode'
    except Exception as e:
        services['redis'] = f'error: {str(e)}'
        status = 'degraded'
    
    return {
        'status': status,
        'timestamp': datetime.utcnow().isoformat(),
        'services': services,
        'kafka_topics': list(TOPICS.values()) if kafka_producer else 'disabled'
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
        'kafka_enabled': kafka_producer is not None
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
                {'date': '2025-06-30T09:15:00Z', 'amount': 2800.00, 'description': 'Salary Deposit'}
            ]
        }
    else:
        result = {'program': program, 'status': 'SUCCESS'}
    
    # Log to Kafka for audit trail
    audit_data = {
        'job_id': job_id,
        'event_type': 'legacy_system_call',
        'program': program,
        'requesting_team': requesting_team,
        'timestamp': datetime.utcnow().isoformat(),
        'request_data': data
    }
    
    if kafka_producer:
        try:
            kafka_producer.send(TOPICS['LEGACY_REQUESTS'], audit_data)
            kafka_producer.send(TOPICS['AUDIT_LOG'], audit_data)
        except Exception as e:
            print(f"❌ Failed to log to Kafka: {e}")
    else:
        audit_logs.append(audit_data)
    
    return {
        'job_id': job_id,
        'program': program,
        'requesting_team': requesting_team,
        'result': result,
        'timestamp': datetime.utcnow().isoformat(),
        'kafka_logged': kafka_producer is not None
    }

@app.route('/api/v1/bulk/extract', methods=['POST'])
def bulk_extract():
    data = request.json or {}
    job_id = str(uuid.uuid4())
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Log bulk extract request
    audit_data = {
        'job_id': job_id,
        'event_type': 'bulk_extract_request',
        'requesting_team': requesting_team,
        'source': data.get('source', 'demo'),
        'filters': data.get('filters', {}),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if kafka_producer:
        try:
            kafka_producer.send(TOPICS['AUDIT_LOG'], audit_data)
        except Exception as e:
            print(f"❌ Failed to log to Kafka: {e}")
    else:
        audit_logs.append(audit_data)
    
    return {
        'job_id': job_id,
        'status': 'completed',
        'source': data.get('source', 'demo'),
        'records': 50,
        'message': 'Bulk extraction completed - file transfers eliminated!',
        'kafka_logged': kafka_producer is not None
    }

# Enhanced CSV Upload with Kafka
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
        
        # Store in Kafka for durability
        if kafka_producer:
            try:
                # Send upload request to Kafka
                upload_event = {
                    'job_id': job_id,
                    'event_type': 'csv_upload',
                    'team': requesting_team,
                    'filename': csv_file.filename,
                    'record_count': len(df),
                    'timestamp': datetime.utcnow().isoformat()
                }
                kafka_producer.send(TOPICS['CSV_UPLOAD'], upload_event)
                
                # Store processed data in Kafka
                kafka_producer.send(TOPICS['CSV_PROCESSED'], {
                    'job_id': job_id,
                    'data': processed_data
                })
                
                # Store metadata in Redis for quick access
                if redis_client:
                    redis_client.setex(f'csv_meta:{job_id}', 3600, json.dumps({
                        'filename': csv_file.filename,
                        'record_count': len(df),
                        'uploaded_by': requesting_team,
                        'timestamp': datetime.utcnow().isoformat()
                    }))
                
                print(f"✅ CSV data stored in Kafka: {job_id}")
                
            except Exception as e:
                print(f"❌ Kafka storage failed, using fallback: {e}")
                csv_storage[job_id] = processed_data
        else:
            # Fallback to in-memory storage
            csv_storage[job_id] = processed_data
        
        return {
            'job_id': job_id,
            'status': 'processed',
            'message': 'CSV uploaded and processed successfully',
            'total_records': len(df),
            'columns': list(df.columns),
            'api_url': f'/api/v1/csv/download/{job_id}',
            'timestamp': datetime.utcnow().isoformat(),
            'kafka_enabled': kafka_producer is not None,
            'storage_backend': 'kafka' if kafka_producer else 'memory'
        }
        
    except Exception as e:
        return {'error': f'CSV processing failed: {str(e)}'}, 500

# Enhanced CSV Download with Kafka
@app.route('/api/v1/csv/download/<job_id>', methods=['GET'])
def csv_download(job_id):
    """System B downloads processed CSV data from Kafka"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Log download request
    download_event = {
        'job_id': job_id,
        'event_type': 'csv_download_request',
        'requesting_team': requesting_team,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    if kafka_producer:
        try:
            kafka_producer.send(TOPICS['CSV_DOWNLOAD'], download_event)
        except Exception as e:
            print(f"❌ Failed to log download request: {e}")
    
    # Get data from Kafka or fallback
    data = get_csv_data_from_kafka(job_id) if kafka_producer else csv_storage.get(job_id)
    
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

# Enhanced CSV Data API with Kafka
@app.route('/api/v1/csv/data/<job_id>', methods=['GET'])
def csv_data_api(job_id):
    """System B gets data as JSON API response from Kafka"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Get data from Kafka or fallback
    data = get_csv_data_from_kafka(job_id) if kafka_producer else csv_storage.get(job_id)
    
    if not data:
        return {'error': 'Job not found or expired'}, 404
    
    # Apply filters
    filters = request.args.to_dict()
    records = data['records']
    
    if filters:
        df = pd.DataFrame(records)
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
            'uploaded_by': data.get('uploaded_by', 'unknown'),
            'columns': data['columns']
        },
        'storage_backend': 'kafka' if kafka_producer else 'memory',
        'requesting_team': requesting_team
    }

# New Audit Logs Endpoint
@app.route('/api/v1/audit/logs', methods=['GET'])
def audit_logs_api():
    """Get audit logs from Kafka or memory"""
    
    limit = int(request.args.get('limit', 100))
    
    if kafka_producer:
        # In production, you'd implement proper Kafka consumer for audit logs
        logs = get_recent_audit_logs_from_kafka(limit)
    else:
        logs = audit_logs[-limit:] if audit_logs else []
    
    return {
        'audit_logs': logs,
        'total_logs': len(logs),
        'storage_backend': 'kafka' if kafka_producer else 'memory',
        'timestamp': datetime.utcnow().isoformat()
    }

# Kafka helper functions
def get_csv_data_from_kafka(job_id):
    """Retrieve CSV data from Kafka topic"""
    try:
        consumer = KafkaConsumer(
            TOPICS['CSV_PROCESSED'],
            bootstrap_servers=[KAFKA_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000,
            auto_offset_reset='earliest'
        )
        
        for message in consumer:
            data = message.value
            if data.get('job_id') == job_id:
                consumer.close()
                return data.get('data')
        
        consumer.close()
        return None
        
    except Exception as e:
        print(f"❌ Error retrieving from Kafka: {e}")
        return None

def get_recent_audit_logs_from_kafka(limit=100):
    """Retrieve recent audit logs from Kafka"""
    try:
        consumer = KafkaConsumer(
            TOPICS['AUDIT_LOG'],
            bootstrap_servers=[KAFKA_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000,
            auto_offset_reset='latest'
        )
        
        logs = []
        for message in consumer:
            logs.append(message.value)
            if len(logs) >= limit:
                break
        
        consumer.close()
        return logs
        
    except Exception as e:
        print(f"❌ Error retrieving audit logs: {e}")
        return []

# Background Kafka consumer (for production)
def start_background_consumer():
    """Start background Kafka consumer for processing"""
    if not kafka_producer:
        return
    
    def consume_messages():
        try:
            consumer = KafkaConsumer(
                TOPICS['CSV_UPLOAD'],
                TOPICS['CSV_DOWNLOAD'],
                bootstrap_servers=[KAFKA_SERVERS],
                group_id='banking-api-consumer',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            print("🔄 Background Kafka consumer started")
            
            for message in consumer:
                print(f"📨 Processed Kafka message: {message.topic} - {message.value.get('event_type')}")
                
        except Exception as e:
            print(f"❌ Background consumer error: {e}")
    
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

if __name__ == '__main__':
    print("🚀 Banking Bulk API Platform Starting...")
    print("📋 Features:")
    print("   • Kafka-enabled message streaming")
    print("   • Redis caching layer")
    print("   • Real-time CSV processing")
    print("   • Complete audit trails")
    print("   • Legacy system integration")
    
    # Start background consumer
    start_background_consumer()
    
    port = int(os.environ.get('PORT', 5000))
    print(f"🌐 API Server starting on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)