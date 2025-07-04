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

# Use confluent-kafka with proper error handling
try:
    from confluent_kafka import Producer, Consumer, KafkaError
    KAFKA_AVAILABLE = True
    print("Confluent Kafka library loaded successfully")
except ImportError as e:
    print(f"Kafka library not available: {e}")
    KAFKA_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

print(f"Starting Banking Bulk API Platform")
print(f"Kafka: {KAFKA_SERVERS} (Available: {KAFKA_AVAILABLE})")

# Kafka Topics
TOPICS = {
    'CSV_UPLOAD': 'csv-upload-requests',
    'CSV_PROCESSED': 'csv-processed-data', 
    'CSV_DOWNLOAD': 'csv-download-requests',
    'AUDIT_LOG': 'system-audit-logs',
    'LEGACY_REQUESTS': 'legacy-system-requests'
}

# In-memory storage for Railway (with backup)
csv_storage = {}
kafka_message_cache = {}  # Cache Kafka messages for quick access
audit_logs = []

# Initialize Kafka Producer
def init_kafka_producer():
    if not KAFKA_AVAILABLE:
        print("Kafka not available, using fallback mode")
        return None
        
    try:
        producer = Producer({
            'bootstrap.servers': KAFKA_SERVERS,
            'client.id': 'banking-api-producer',
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 60000
        })
        print("Kafka producer connected")
        return producer
    except Exception as e:
        print(f"Kafka producer failed: {e}")
        return None

# Initialize Kafka Consumer
def init_kafka_consumer():
    if not KAFKA_AVAILABLE:
        return None
        
    try:
        consumer = Consumer({
            'bootstrap.servers': KAFKA_SERVERS,
            'group.id': 'banking-api-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'request.timeout.ms': 30000
        })
        print("Kafka consumer initialized")
        return consumer
    except Exception as e:
        print(f"Kafka consumer failed: {e}")
        return None

# Initialize connections
kafka_producer = init_kafka_producer()
kafka_consumer = init_kafka_consumer()

# Enhanced Kafka send function
def send_to_kafka(topic, message, key=None):
    """Send message to Kafka with enhanced error handling"""
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
            callback=delivery_report
        )
        
        # Flush to ensure delivery
        kafka_producer.flush(timeout=5)
        
        # Also cache the message for immediate access
        if key:
            kafka_message_cache[key] = message
        
        return True
        
    except Exception as e:
        print(f"Failed to send to Kafka: {e}")
        return False

def delivery_report(err, msg):
    """Kafka delivery callback"""
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(f"Kafka message delivered: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# Enhanced Kafka consumer function
def get_csv_data_from_kafka(job_id):
    """Retrieve CSV data from Kafka with multiple strategies"""
    
    # Strategy 1: Check cache first (fastest)
    if job_id in kafka_message_cache:
        print(f"Found data in Kafka cache for job: {job_id}")
        return kafka_message_cache[job_id].get('data')
    
    # Strategy 2: Try to consume from Kafka topic
    if not kafka_consumer:
        print("No Kafka consumer available")
        return None
    
    try:
        # Subscribe to the processed data topic
        kafka_consumer.subscribe([TOPICS['CSV_PROCESSED']])
        
        # Poll for messages
        timeout = 10  # 10 seconds timeout
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            msg = kafka_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break
            
            try:
                # Parse message
                message_data = json.loads(msg.value().decode('utf-8'))
                
                # Check if this is the job we're looking for
                if message_data.get('job_id') == job_id:
                    print(f"Found data in Kafka for job: {job_id}")
                    
                    # Cache it for future access
                    kafka_message_cache[job_id] = message_data
                    
                    return message_data.get('data')
                    
            except json.JSONDecodeError as e:
                print(f"Failed to parse Kafka message: {e}")
                continue
        
        print(f"Data not found in Kafka for job: {job_id}")
        return None
        
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        return None

@app.route('/')
def index():
    return {
        'service': 'SCB Banking Bulk API Platform',
        'version': '2.1.0',
        'status': 'live',
        'architecture': 'Kafka-enabled' if kafka_producer else 'Fallback-mode',
        'message': 'Replacing file transfers with real-time APIs',
        'kafka_enabled': kafka_producer is not None,
        'kafka_consumer_enabled': kafka_consumer is not None,
        'demo_endpoints': {
            'health': '/health',
            'teams': '/api/v1/teams',
            'legacy_cobol': '/api/v1/legacy/CUSTBAL',
            'bulk_extract': '/api/v1/bulk/extract',
            'csv_upload': '/api/v1/csv/upload',
            'csv_download': '/api/v1/csv/download/{job_id}',
            'audit_logs': '/api/v1/audit/logs',
            'kafka_status': '/api/v1/kafka/status'
        }
    }

@app.route('/health')
def health():
    status = 'healthy'
    services = {}
    
    # Check Kafka Producer
    if kafka_producer:
        try:
            # Test Kafka by sending a health check message
            test_sent = send_to_kafka('health-check', {
                'timestamp': datetime.utcnow().isoformat(),
                'service': 'banking-api'
            })
            services['kafka_producer'] = 'connected' if test_sent else 'error'
        except Exception as e:
            services['kafka_producer'] = f'error: {str(e)}'
            status = 'degraded'
    else:
        services['kafka_producer'] = 'fallback_mode'
        status = 'degraded'
    
    # Check Kafka Consumer
    if kafka_consumer:
        services['kafka_consumer'] = 'connected'
    else:
        services['kafka_consumer'] = 'fallback_mode'
        status = 'degraded'
    
    return {
        'status': status,
        'timestamp': datetime.utcnow().isoformat(),
        'services': services,
        'kafka_topics': list(TOPICS.values()) if kafka_producer else 'disabled',
        'storage_mode': 'kafka+cache' if kafka_producer else 'memory',
        'cached_jobs': len(kafka_message_cache)
    }

@app.route('/api/v1/kafka/status')
def kafka_status():
    """Detailed Kafka status endpoint"""
    return {
        'kafka_producer': kafka_producer is not None,
        'kafka_consumer': kafka_consumer is not None,
        'kafka_available': KAFKA_AVAILABLE,
        'topics': TOPICS,
        'cached_messages': len(kafka_message_cache),
        'cached_jobs': list(kafka_message_cache.keys()),
        'memory_storage': len(csv_storage),
        'timestamp': datetime.utcnow().isoformat()
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
    
    audit_data = {
        'job_id': job_id,
        'event_type': 'bulk_extract_request',
        'requesting_team': requesting_team,
        'source': data.get('source', 'demo'),
        'filters': data.get('filters', {}),
        'timestamp': datetime.utcnow().isoformat()
    }
    
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
        
        # Store in Kafka with enhanced error handling
        kafka_logged = False
        
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
            
            # Store processed data in Kafka
            if kafka_logged:
                kafka_message = {
                    'job_id': job_id,
                    'data': processed_data,
                    'timestamp': datetime.utcnow().isoformat()
                }
                kafka_logged = send_to_kafka(TOPICS['CSV_PROCESSED'], kafka_message, job_id)
                
                # IMPORTANT: Also cache immediately for quick access
                if kafka_logged:
                    kafka_message_cache[job_id] = kafka_message
                    print(f"Data cached for immediate access: {job_id}")
        
        # Always store in memory as backup
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
            'kafka_logged': kafka_logged,
            'storage_backend': 'kafka+cache+memory' if kafka_logged else 'memory'
        }
        
    except Exception as e:
        return {'error': f'CSV processing failed: {str(e)}'}, 500

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
        send_to_kafka(TOPICS['CSV_DOWNLOAD'], download_event, job_id)
    
    # Try multiple strategies to get data
    data = None
    source = 'unknown'
    
    # Strategy 1: Kafka cache (fastest)
    if job_id in kafka_message_cache:
        data = kafka_message_cache[job_id].get('data')
        source = 'kafka_cache'
        print(f"Retrieved from Kafka cache: {job_id}")
    
    # Strategy 2: Try Kafka consumer
    if not data and kafka_consumer:
        data = get_csv_data_from_kafka(job_id)
        if data:
            source = 'kafka_consumer'
            print(f"Retrieved from Kafka consumer: {job_id}")
    
    # Strategy 3: Memory fallback
    if not data:
        data = csv_storage.get(job_id)
        if data:
            source = 'memory'
            print(f"Retrieved from memory: {job_id}")
    
    if not data:
        return {'error': f'Job not found: {job_id}', 'searched_in': ['kafka_cache', 'kafka_consumer', 'memory']}, 404
    
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
        response.headers['X-Data-Source'] = source
        
        return response
        
    except Exception as e:
        return {'error': f'CSV generation failed: {str(e)}'}, 500

@app.route('/api/v1/csv/data/<job_id>', methods=['GET'])
def csv_data_api(job_id):
    """System B gets data as JSON API response"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    
    # Try multiple strategies to get data
    data = None
    source = 'unknown'
    
    # Strategy 1: Kafka cache (fastest)
    if job_id in kafka_message_cache:
        data = kafka_message_cache[job_id].get('data')
        source = 'kafka_cache'
        print(f"Retrieved from Kafka cache: {job_id}")
    
    # Strategy 2: Try Kafka consumer
    if not data and kafka_consumer:
        data = get_csv_data_from_kafka(job_id)
        if data:
            source = 'kafka_consumer'
            print(f"Retrieved from Kafka consumer: {job_id}")
    
    # Strategy 3: Memory fallback
    if not data:
        data = csv_storage.get(job_id)
        if data:
            source = 'memory'
            print(f"Retrieved from memory: {job_id}")
    
    if not data:
        return {
            'error': f'Job not found: {job_id}',
            'searched_in': ['kafka_cache', 'kafka_consumer', 'memory'],
            'available_jobs': list(kafka_message_cache.keys()) + list(csv_storage.keys())
        }, 404
    
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
                    print(f"Filter error for {key}={value}: {e}")
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
        'storage_backend': source,
        'requesting_team': requesting_team,
        'kafka_enabled': kafka_producer is not None
    }

@app.route('/api/v1/audit/logs', methods=['GET'])
def audit_logs_api():
    """Get audit logs"""
    
    limit = int(request.args.get('limit', 10))
    logs = audit_logs[-limit:] if audit_logs else []
    
    return {
        'audit_logs': logs,
        'total_logs': len(logs),
        'storage_backend': 'kafka' if kafka_producer else 'memory',
        'timestamp': datetime.utcnow().isoformat(),
        'kafka_enabled': kafka_producer is not None
    }

if __name__ == '__main__':
    print("Banking Bulk API Platform Starting...")
    print("Features:")
    print(f"   • Kafka messaging: {'Enabled' if kafka_producer else 'Fallback mode'}")
    print(f"   • Kafka consumer: {'Enabled' if kafka_consumer else 'Fallback mode'}")
    print("   • Multi-tier storage: Kafka + Cache + Memory")
    print("   • Real-time CSV processing")
    print("   • Complete audit trails")
    print("   • Legacy system integration")
    
    port = int(os.environ.get('PORT', 5000))
    print(f"API Server starting on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)