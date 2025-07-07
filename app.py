from flask import Flask, request, jsonify, send_file, make_response, Response
from flask_cors import CORS
import json
import uuid
import os
import threading
import time
import tempfile
from datetime import datetime
import pandas as pd
import io
from werkzeug.utils import secure_filename

# Use confluent-kafka with proper error handling
try:
    from confluent_kafka import Producer, Consumer, KafkaError
    KAFKA_AVAILABLE = True
    print("Confluent Kafka library loaded successfully")
except ImportError as e:
    print(f"Kafka library not available: {e}")
    KAFKA_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
    print("Redis library loaded successfully")
except ImportError as e:
    print(f"Redis library not available: {e}")
    REDIS_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')

# Safe environment variable parsing with error handling
try:
    MAX_CHUNK_SIZE = int(os.getenv('MAX_CHUNK_SIZE', '10000'))
except ValueError:
    print("Warning: Invalid MAX_CHUNK_SIZE environment variable, using default 10000")
    MAX_CHUNK_SIZE = 10000

try:
    KAFKA_MAX_MESSAGE_SIZE = int(os.getenv('KAFKA_MAX_MESSAGE_SIZE', '900000'))
except ValueError:
    print("Warning: Invalid KAFKA_MAX_MESSAGE_SIZE environment variable, using default 900000")
    KAFKA_MAX_MESSAGE_SIZE = 900000

print(f"Starting Banking Bulk API Platform")
print(f"Kafka: {KAFKA_SERVERS} (Available: {KAFKA_AVAILABLE})")
print(f"Redis: {REDIS_URL} (Available: {REDIS_AVAILABLE})")
print(f"Max chunk size: {MAX_CHUNK_SIZE} records")

# Kafka Topics
TOPICS = {
    'CSV_UPLOAD': 'csv-upload-requests',
    'CSV_PROCESSED': 'csv-processed-data', 
    'CSV_DOWNLOAD': 'csv-download-requests',
    'AUDIT_LOG': 'system-audit-logs',
    'LEGACY_REQUESTS': 'legacy-system-requests',
    'CSV_JOB_METADATA': 'csv-job-metadata'
}

# Storage
csv_storage = {}
csv_chunks_storage = {}
job_metadata_storage = {}
kafka_message_cache = {}
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
            'delivery.timeout.ms': 60000,
            'message.max.bytes': KAFKA_MAX_MESSAGE_SIZE
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
            'request.timeout.ms': 30000,
            'fetch.message.max.bytes': KAFKA_MAX_MESSAGE_SIZE
        })
        print("Kafka consumer initialized")
        return consumer
    except Exception as e:
        print(f"Kafka consumer failed: {e}")
        return None

# Initialize Redis
def init_redis_client():
    if not REDIS_AVAILABLE:
        print("Redis not available, using fallback mode")
        return None
        
    try:
        client = redis.from_url(REDIS_URL)
        client.ping()
        print("Redis connected")
        return client
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return None

# Initialize connections
kafka_producer = init_kafka_producer()
kafka_consumer = init_kafka_consumer()
redis_client = init_redis_client()

# Enhanced Kafka send function
def send_to_kafka(topic, message, key=None):
    """Send message to Kafka with enhanced error handling"""
    if not kafka_producer:
        return False
    
    try:
        message_json = json.dumps(message)
        
        # Check message size
        if len(message_json.encode('utf-8')) > KAFKA_MAX_MESSAGE_SIZE:
            print(f"Message too large for Kafka: {len(message_json)} bytes")
            return False
        
        kafka_producer.produce(
            topic=topic,
            key=key,
            value=message_json,
            callback=delivery_report
        )
        
        kafka_producer.flush(timeout=5)
        
        # Cache message for immediate access
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

# Storage helper functions
def store_csv_chunk(job_id, chunk_id, chunk_data):
    """Store CSV chunk in multiple backends"""
    chunk_key = f"{job_id}_{chunk_id}"
    
    # Store in memory (always)
    csv_chunks_storage[chunk_key] = chunk_data
    
    # Try to store in Kafka
    if kafka_producer:
        kafka_topic = f"csv-chunks-{job_id}"
        success = send_to_kafka(kafka_topic, chunk_data, chunk_key)
        if success:
            print(f"Chunk stored in Kafka: {chunk_key}")
    
    # Try to store in Redis
    if redis_client:
        try:
            redis_client.setex(f"chunk:{chunk_key}", 3600, json.dumps(chunk_data))
            print(f"Chunk cached in Redis: {chunk_key}")
        except Exception as e:
            print(f"Redis chunk storage failed: {e}")

def get_csv_chunk(job_id, chunk_id):
    """Retrieve CSV chunk from multiple backends"""
    chunk_key = f"{job_id}_{chunk_id}"
    
    # Try memory first (fastest)
    if chunk_key in csv_chunks_storage:
        print(f"Chunk retrieved from memory: {chunk_key}")
        return csv_chunks_storage[chunk_key]
    
    # Try Redis cache
    if redis_client:
        try:
            cached_data = redis_client.get(f"chunk:{chunk_key}")
            if cached_data:
                chunk_data = json.loads(cached_data)
                csv_chunks_storage[chunk_key] = chunk_data  # Cache in memory
                print(f"Chunk retrieved from Redis: {chunk_key}")
                return chunk_data
        except Exception as e:
            print(f"Redis chunk retrieval failed: {e}")
    
    # Try Kafka (slowest but most reliable)
    if kafka_consumer:
        try:
            kafka_topic = f"csv-chunks-{job_id}"
            kafka_consumer.subscribe([kafka_topic])
            
            timeout = 10
            end_time = time.time() + timeout
            
            while time.time() < end_time:
                msg = kafka_consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        break
                
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    if message_data.get('chunk_id') == chunk_id:
                        csv_chunks_storage[chunk_key] = message_data  # Cache in memory
                        print(f"Chunk retrieved from Kafka: {chunk_key}")
                        return message_data
                except json.JSONDecodeError:
                    continue
                    
        except Exception as e:
            print(f"Kafka chunk retrieval failed: {e}")
    
    print(f"Chunk not found: {chunk_key}")
    return None

def store_job_metadata(job_id, metadata):
    """Store job metadata in multiple backends with immediate availability"""
    # Always store in memory first for immediate access
    job_metadata_storage[job_id] = metadata
    print(f"✅ Job metadata stored in memory: {job_id}")
    
    # Store in Kafka
    if kafka_producer:
        kafka_success = send_to_kafka(TOPICS['CSV_JOB_METADATA'], metadata, job_id)
        if kafka_success:
            print(f"✅ Job metadata sent to Kafka: {job_id}")
    
    # Store in Redis
    if redis_client:
        try:
            redis_client.setex(f"job:{job_id}", 7200, json.dumps(metadata))  # 2 hour TTL
            print(f"✅ Job metadata cached in Redis: {job_id}")
        except Exception as e:
            print(f"❌ Redis job metadata storage failed: {e}")

def get_job_metadata(job_id):
    """Retrieve job metadata from multiple backends with fallback"""
    # Try memory first (fastest and most reliable)
    if job_id in job_metadata_storage:
        print(f"✅ Job metadata retrieved from memory: {job_id}")
        return job_metadata_storage[job_id]
    
    # Try Redis cache
    if redis_client:
        try:
            cached_data = redis_client.get(f"job:{job_id}")
            if cached_data:
                metadata = json.loads(cached_data)
                job_metadata_storage[job_id] = metadata  # Cache in memory
                print(f"✅ Job metadata retrieved from Redis: {job_id}")
                return metadata
        except Exception as e:
            print(f"❌ Redis job metadata retrieval failed: {e}")
    
    # Try Kafka cache (from recent messages)
    if job_id in kafka_message_cache:
        kafka_data = kafka_message_cache[job_id]
        if isinstance(kafka_data, dict) and 'job_id' in kafka_data:
            job_metadata_storage[job_id] = kafka_data  # Cache in memory
            print(f"✅ Job metadata retrieved from Kafka cache: {job_id}")
            return kafka_data
    
    print(f"❌ Job metadata not found: {job_id}")
    return None

# def get_job_metadata(job_id):
#     """Retrieve job metadata from multiple backends"""
#     # Try memory first
#     if job_id in job_metadata_storage:
#         return job_metadata_storage[job_id]
    
#     # Try Redis
#     if redis_client:
#         try:
#             cached_data = redis_client.get(f"job:{job_id}")
#             if cached_data:
#                 metadata = json.loads(cached_data)
#                 job_metadata_storage[job_id] = metadata  # Cache in memory
#                 return metadata
#         except Exception as e:
#             print(f"Redis job metadata retrieval failed: {e}")
    
#     # Try Kafka consumer for metadata
#     if kafka_consumer:
#         try:
#             kafka_consumer.subscribe([TOPICS['CSV_JOB_METADATA']])
            
#             timeout = 10
#             end_time = time.time() + timeout
            
#             while time.time() < end_time:
#                 msg = kafka_consumer.poll(timeout=1.0)
                
#                 if msg is None:
#                     continue
                    
#                 if msg.error():
#                     if msg.error().code() == KafkaError._PARTITION_EOF:
#                         continue
#                     else:
#                         break
                
#                 try:
#                     metadata = json.loads(msg.value().decode('utf-8'))
#                     if metadata.get('job_id') == job_id:
#                         job_metadata_storage[job_id] = metadata  # Cache in memory
#                         return metadata
#                 except json.JSONDecodeError:
#                     continue
                    
#         except Exception as e:
#             print(f"Kafka job metadata retrieval failed: {e}")
    
#     return None

@app.route('/')
def index():
    return {
        'service': 'SCB Banking Bulk API Platform',
        'version': '3.0.0',
        'status': 'live',
        'architecture': 'Large-file-enabled',
        'message': 'Replacing file transfers with real-time APIs',
        'features': [
            'Large file processing (streaming)',
            'Kafka message persistence',
            'Redis caching layer',
            'Chunked data processing',
            'Paginated API responses',
            'Legacy system integration'
        ],
        'kafka_enabled': kafka_producer is not None,
        'redis_enabled': redis_client is not None,
        'max_chunk_size': MAX_CHUNK_SIZE,
        'demo_endpoints': {
            'health': '/health',
            'teams': '/api/v1/teams',
            'legacy_cobol': '/api/v1/legacy/CUSTBAL',
            'bulk_extract': '/api/v1/bulk/extract',
            'csv_upload': '/api/v1/csv/upload',
            'csv_upload_streaming': '/api/v1/csv/upload/stream',
            'csv_download': '/api/v1/csv/download/{job_id}',
            'csv_data_paginated': '/api/v1/csv/data/{job_id}?page=1&per_page=1000',
            'job_status': '/api/v1/csv/jobs/{job_id}/status',
            'audit_logs': '/api/v1/audit/logs'
        }
    }

@app.route('/health')
def health():
    status = 'healthy'
    services = {}
    
    # Check Kafka Producer
    if kafka_producer:
        try:
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
        'storage_stats': {
            'memory_jobs': len(job_metadata_storage),
            'memory_chunks': len(csv_chunks_storage),
            'kafka_cache': len(kafka_message_cache)
        },
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
        'kafka_logged': kafka_logged
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
        'kafka_logged': kafka_logged
    }

# ORIGINAL CSV UPLOAD (for smaller files)
@app.route('/api/v1/csv/upload', methods=['POST'])
def csv_upload():
    """Original CSV upload for smaller files (under 50MB)"""
    # DEBUG: Print request details
    print("=== DEBUG CSV UPLOAD ===")
    print(f"Request method: {request.method}")
    print(f"Request files: {request.files}")
    print(f"Request form: {request.form}")
    print(f"Request headers: {dict(request.headers)}")
    print(f"Content type: {request.content_type}")
    print("========================")
    
    if 'file' not in request.files:
        return {'error': 'No CSV file provided'}, 400
    
    csv_file = request.files['file']
    if csv_file.filename == '':
        return {'error': 'No file selected'}, 400
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    job_id = str(uuid.uuid4())
    
    try:
        # Check file size (estimate)
        csv_file.seek(0, 2)  # Seek to end
        file_size = csv_file.tell()
        csv_file.seek(0)  # Reset to beginning
        
        # If file is large, recommend streaming endpoint
        if file_size > 50 * 1024 * 1024:  # 50MB
            return {
                'error': 'File too large for standard upload',
                'file_size_mb': round(file_size / (1024 * 1024), 1),
                'recommendation': 'Use /api/v1/csv/upload/stream endpoint for large files',
                'max_size_mb': 50
            }, 413
        
        # Read and process CSV (original method for smaller files)
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
            'processing_method': 'standard',
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'status': 'ready'
        }
        
        # Store using multiple methods
        kafka_logged = False
        
        if kafka_producer:
            # For small files, store directly in Kafka
            kafka_message = {
                'job_id': job_id,
                'data': processed_data,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Check if message size is acceptable for Kafka
            message_size = len(json.dumps(kafka_message).encode('utf-8'))
            if message_size < KAFKA_MAX_MESSAGE_SIZE:
                kafka_logged = send_to_kafka(TOPICS['CSV_PROCESSED'], kafka_message, job_id)
                if kafka_logged:
                    kafka_message_cache[job_id] = kafka_message
            else:
                print(f"Message too large for Kafka ({message_size} bytes), using chunks")
                # Fall back to chunking for large data
                return csv_upload_streaming_internal(csv_file, requesting_team, job_id)
        
        # Always store in memory as backup
        csv_storage[job_id] = processed_data

        # Store job metadata for status endpoint
        job_metadata = {
            'job_id': job_id,
            'filename': csv_file.filename,
            'total_records': len(df),
            'headers': list(df.columns),
            'uploaded_at': datetime.utcnow().isoformat(),
            'uploaded_by': requesting_team,
            'processing_method': 'standard',
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'status': 'completed'
        }
        store_job_metadata(job_id, job_metadata)
        
        return {
            'job_id': job_id,
            'status': 'processed',
            'message': 'CSV uploaded and processed successfully',
            'total_records': len(df),
            'columns': list(df.columns),
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'processing_method': 'standard',
            'api_url': f'/api/v1/csv/download/{job_id}',
            'timestamp': datetime.utcnow().isoformat(),
            'kafka_enabled': kafka_producer is not None,
            'kafka_logged': kafka_logged,
            'storage_backend': 'kafka+memory' if kafka_logged else 'memory'
        }
        
    except Exception as e:
        return {'error': f'CSV processing failed: {str(e)}'}, 500

# NEW STREAMING CSV UPLOAD (for large files)
@app.route('/api/v1/csv/upload/stream', methods=['POST'])
def csv_upload_streaming():
    """Streaming CSV upload for large files (GB scale)"""
    # DEBUG: Print request details
    print("=== DEBUG CSV UPLOAD ===")
    print(f"Request method: {request.method}")
    print(f"Request files: {request.files}")
    print(f"Request form: {request.form}")
    print(f"Request headers: {dict(request.headers)}")
    print(f"Content type: {request.content_type}")
    print("========================")
    
    if 'file' not in request.files:
        return {'error': 'No CSV file provided'}, 400
    
    csv_file = request.files['file']
    if csv_file.filename == '':
        return {'error': 'No file selected'}, 400
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    job_id = str(uuid.uuid4())
    
    return csv_upload_streaming_internal(csv_file, requesting_team, job_id)

def csv_upload_streaming_internal(csv_file, requesting_team, job_id):
    """Internal streaming processing function"""
    
    try:
        # Save file temporarily for streaming processing
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix='.csv') as temp_file:
            csv_file.save(temp_file.name)
            temp_file_path = temp_file.name
        
        processing_start = time.time()
        
        try:
            # Stream processing
            chunk_count = 0
            total_records = 0
            headers = None
            
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                # Read header
                header_line = next(f).strip()
                headers = [col.strip().strip('"') for col in header_line.split(',')]
                
                # Process in chunks
                chunk_records = []
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    
                    # Parse line
                    values = [val.strip().strip('"') for val in line.strip().split(',')]
                    
                    # Handle different numbers of columns
                    if len(values) != len(headers):
                        # Pad or truncate to match headers
                        if len(values) < len(headers):
                            values.extend([''] * (len(headers) - len(values)))
                        else:
                            values = values[:len(headers)]
                    
                    record = dict(zip(headers, values))
                    chunk_records.append(record)
                    total_records += 1
                    
                    # Process chunk when it reaches size limit
                    if len(chunk_records) >= MAX_CHUNK_SIZE:
                        chunk_count += 1
                        
                        chunk_data = {
                            'job_id': job_id,
                            'chunk_id': chunk_count,
                            'records': chunk_records,
                            'chunk_size': len(chunk_records),
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        
                        # Store chunk
                        store_csv_chunk(job_id, chunk_count, chunk_data)
                        
                        # Clear chunk for next batch
                        chunk_records = []
                        
                        print(f"Processed chunk {chunk_count}: {MAX_CHUNK_SIZE} records")
                
                # Process final chunk if exists
                if chunk_records:
                    chunk_count += 1
                    chunk_data = {
                        'job_id': job_id,
                        'chunk_id': chunk_count,
                        'records': chunk_records,
                        'chunk_size': len(chunk_records),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    store_csv_chunk(job_id, chunk_count, chunk_data)
            
            processing_time = time.time() - processing_start
            
            # Get file size
            file_size = os.path.getsize(temp_file_path)
            
            # Store job metadata
            job_metadata = {
                'job_id': job_id,
                'filename': csv_file.filename,
                'total_records': total_records,
                'total_chunks': chunk_count,
                'headers': headers,
                'uploaded_at': datetime.utcnow().isoformat(),
                'uploaded_by': requesting_team,
                'processing_method': 'streaming',
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'processing_time_seconds': round(processing_time, 2),
                'records_per_second': round(total_records / processing_time) if processing_time > 0 else 0,
                'status': 'completed'
            }
            
            store_job_metadata(job_id, job_metadata)
            
            return {
                'job_id': job_id,
                'status': 'completed',
                'message': 'Large CSV processed successfully using streaming',
                'total_records': total_records,
                'total_chunks': chunk_count,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'processing_method': 'streaming',
                'processing_time_seconds': round(processing_time, 2),
                'records_per_second': round(total_records / processing_time) if processing_time > 0 else 0,
                'headers': headers,
                'api_url': f'/api/v1/csv/download/{job_id}',
                'stream_url': f'/api/v1/csv/stream/{job_id}',
                'status_url': f'/api/v1/csv/jobs/{job_id}/status',
                'timestamp': datetime.utcnow().isoformat(),
                'kafka_enabled': kafka_producer is not None,
                'redis_enabled': redis_client is not None
            }
            
        finally:
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        return {'error': f'Streaming processing failed: {str(e)}'}, 500

@app.route('/api/v1/csv/download/<job_id>', methods=['GET'])
def csv_download(job_id):
    """Download CSV data (supports both standard and streaming jobs)"""
    
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
    
    # Check if this is a chunked job (streaming) or standard job
    metadata = get_job_metadata(job_id)
    
    if metadata and metadata.get('processing_method') == 'streaming':
        # Handle streaming download
        return csv_download_streaming(job_id, metadata)
    else:
        # Handle standard download
        return csv_download_standard(job_id)

def csv_download_standard(job_id):
    """Standard CSV download for smaller files"""
    
    # Try multiple storage backends
    data = None
    source = 'unknown'
    
    # Try Kafka cache first
    if job_id in kafka_message_cache:
        data = kafka_message_cache[job_id].get('data')
        source = 'kafka_cache'
    
    # Try memory storage
    if not data:
        data = csv_storage.get(job_id)
        if data:
            source = 'memory'
    
    if not data:
        return {'error': f'Job not found: {job_id}'}, 404
    
    try:
        # Convert to CSV
        df = pd.DataFrame(data['records'])
        output = io.StringIO()
        df.to_csv(output, index=False)
        csv_content = output.getvalue()
        
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=download_{data["filename"]}'
        response.headers['X-Data-Source'] = source
        response.headers['X-Total-Records'] = str(len(data['records']))
        
        return response
        
    except Exception as e:
        return {'error': f'CSV generation failed: {str(e)}'}, 500

def csv_download_streaming(job_id, metadata):
    """Streaming CSV download for large files"""
    
    def generate_csv():
        try:
            # Stream header
            headers = metadata['headers']
            yield ','.join(headers) + '\n'
            
            # Stream chunks
            for chunk_id in range(1, metadata['total_chunks'] + 1):
                chunk_data = get_csv_chunk(job_id, chunk_id)
                if chunk_data and 'records' in chunk_data:
                    for record in chunk_data['records']:
                        row = []
                        for header in headers:
                            value = str(record.get(header, ''))
                            # Escape commas and quotes in CSV
                            if ',' in value or '"' in value or '\n' in value:
                                value = '"' + value.replace('"', '""') + '"'
                            row.append(value)
                        yield ','.join(row) + '\n'
        except Exception as e:
            yield f'error,message\n'
            yield f'500,"CSV generation failed: {str(e)}"\n'
    
    response = Response(
        generate_csv(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=download_{metadata["filename"]}',
            'X-Processing-Method': 'streaming',
            'X-Total-Records': str(metadata['total_records']),
            'X-Total-Chunks': str(metadata['total_chunks'])
        }
    )
    
    return response

@app.route('/api/v1/csv/stream/<job_id>')
def csv_stream_download(job_id):
    """Direct streaming endpoint for large files"""
    
    metadata = get_job_metadata(job_id)
    if not metadata:
        return {'error': 'Job not found'}, 404
    
    return csv_download_streaming(job_id, metadata)

@app.route('/api/v1/csv/data/<job_id>', methods=['GET'])
def csv_data_api(job_id):
    """Get CSV data as JSON with pagination support"""
    
    requesting_team = request.headers.get('X-Team', 'unknown-team')
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 1000)), 10000)  # Max 10K per page
    
    # Check if this is a chunked job or standard job
    metadata = get_job_metadata(job_id)
    
    if metadata and metadata.get('processing_method') == 'streaming':
        return csv_data_api_chunked(job_id, metadata, page, per_page, requesting_team)
    else:
        return csv_data_api_standard(job_id, page, per_page, requesting_team)

def csv_data_api_standard(job_id, page, per_page, requesting_team):
    """Standard JSON API for smaller files"""
    
    # Get data from storage
    data = None
    source = 'unknown'
    
    # Try Kafka cache first
    if job_id in kafka_message_cache:
        data = kafka_message_cache[job_id].get('data')
        source = 'kafka_cache'
    
    # Try memory storage
    if not data:
        data = csv_storage.get(job_id)
        if data:
            source = 'memory'
    
    if not data:
        return {'error': f'Job not found: {job_id}'}, 404
    
    # Apply filters
    filters = request.args.to_dict()
    # Remove pagination parameters from filters
    filters.pop('page', None)
    filters.pop('per_page', None)
    
    records = data['records']
    
    if filters:
        df = pd.DataFrame(records)
        for key, value in filters.items():
            if key in df.columns:
                try:
                    if value.startswith('>'):
                        df = df[pd.to_numeric(df[key], errors='coerce') > float(value[1:])]
                    elif value.startswith('<'):
                        df = df[pd.to_numeric(df[key], errors='coerce') < float(value[1:])]
                    else:
                        df = df[df[key].astype(str) == value]
                except Exception as e:
                    print(f"Filter error for {key}={value}: {e}")
        records = df.to_dict('records')
    
    # Paginate
    total_records = len(records)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_records = records[start_idx:end_idx]
    
    total_pages = (total_records + per_page - 1) // per_page
    
    return {
        'job_id': job_id,
        'status': 'success',
        'page': page,
        'per_page': per_page,
        'total_records': total_records,
        'total_pages': total_pages,
        'records_on_page': len(paginated_records),
        'data': paginated_records,
        'metadata': {
            'original_filename': data['filename'],
            'uploaded_at': data['uploaded_at'],
            'uploaded_by': data.get('uploaded_by', 'unknown'),
            'columns': data['columns'],
            'processing_method': 'standard'
        },
        'storage_backend': source,
        'requesting_team': requesting_team,
        'pagination': {
            'has_next': page < total_pages,
            'has_prev': page > 1,
            'next_page': page + 1 if page < total_pages else None,
            'prev_page': page - 1 if page > 1 else None
        }
    }

def csv_data_api_chunked(job_id, metadata, page, per_page, requesting_team):
    """Chunked JSON API for large files"""
    
    # Calculate which chunks to fetch
    start_record = (page - 1) * per_page
    end_record = start_record + per_page
    
    records = []
    current_record = 0
    chunks_accessed = 0
    
    for chunk_id in range(1, metadata['total_chunks'] + 1):
        if len(records) >= per_page:
            break
            
        chunk_data = get_csv_chunk(job_id, chunk_id)
        if not chunk_data or 'records' not in chunk_data:
            continue
        
        chunks_accessed += 1
        
        for record in chunk_data['records']:
            if current_record >= start_record and current_record < end_record:
                records.append(record)
            current_record += 1
            
            if len(records) >= per_page:
                break
    
    total_pages = (metadata['total_records'] + per_page - 1) // per_page
    
    return {
        'job_id': job_id,
        'status': 'success',
        'page': page,
        'per_page': per_page,
        'total_records': metadata['total_records'],
        'total_pages': total_pages,
        'records_on_page': len(records),
        'data': records,
        'metadata': {
            'original_filename': metadata['filename'],
            'uploaded_at': metadata['uploaded_at'],
            'uploaded_by': metadata.get('uploaded_by', 'unknown'),
            'columns': metadata['headers'],
            'processing_method': 'streaming',
            'total_chunks': metadata['total_chunks'],
            'chunks_accessed': chunks_accessed,
            'file_size_mb': metadata.get('file_size_mb', 'unknown'),
            'processing_time_seconds': metadata.get('processing_time_seconds', 'unknown')
        },
        'storage_backend': 'chunked',
        'requesting_team': requesting_team,
        'pagination': {
            'has_next': page < total_pages,
            'has_prev': page > 1,
            'next_page': page + 1 if page < total_pages else None,
            'prev_page': page - 1 if page > 1 else None
        }
    }

@app.route('/api/v1/csv/jobs/<job_id>/status')
def job_status(job_id):
    """Get processing status for jobs"""
    
    metadata = get_job_metadata(job_id)
    if not metadata:
        return {'error': 'Job not found'}, 404
    
    return {
        'job_id': job_id,
        'status': metadata['status'],
        'filename': metadata['filename'],
        'total_records': metadata['total_records'],
        'processing_method': metadata.get('processing_method', 'standard'),
        'file_size_mb': metadata.get('file_size_mb', 'unknown'),
        'uploaded_at': metadata['uploaded_at'],
        'uploaded_by': metadata.get('uploaded_by', 'unknown'),
        'processing_time_seconds': metadata.get('processing_time_seconds', 'unknown'),
        'records_per_second': metadata.get('records_per_second', 'unknown'),
        'total_chunks': metadata.get('total_chunks', 1),
        'headers': metadata.get('headers', metadata.get('columns', [])),
        'endpoints': {
            'download_csv': f'/api/v1/csv/download/{job_id}',
            'data_api': f'/api/v1/csv/data/{job_id}',
            'streaming': f'/api/v1/csv/stream/{job_id}' if metadata.get('processing_method') == 'streaming' else None
        }
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

@app.route('/api/v1/system/stats')
def system_stats():
    """Get system performance statistics"""
    
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'storage_stats': {
            'memory_jobs': len(job_metadata_storage),
            'memory_chunks': len(csv_chunks_storage),
            'memory_storage': len(csv_storage),
            'kafka_cache': len(kafka_message_cache)
        },
        'configuration': {
            'max_chunk_size': MAX_CHUNK_SIZE,
            'kafka_max_message_size': KAFKA_MAX_MESSAGE_SIZE,
            'kafka_enabled': kafka_producer is not None,
            'redis_enabled': redis_client is not None
        },
        'performance': {
            'recommended_chunk_size': MAX_CHUNK_SIZE,
            'max_standard_upload_mb': 50,
            'use_streaming_for_files_larger_than_mb': 50
        }
    }

if __name__ == '__main__':
    print("Banking Bulk API Platform Starting...")
    print("Features:")
    print(f"   • Kafka messaging: {'Enabled' if kafka_producer else 'Fallback mode'}")
    print(f"   • Redis caching: {'Enabled' if redis_client else 'Fallback mode'}")
    print(f"   • Max chunk size: {MAX_CHUNK_SIZE} records")
    print(f"   • Kafka max message: {KAFKA_MAX_MESSAGE_SIZE} bytes")
    print("   • Large file streaming support")
    print("   • Paginated API responses")
    print("   • Multi-tier storage architecture")
    print("   • Legacy system integration")
    print("   • Complete audit trails")
    
    port = int(os.environ.get('PORT', 5000))
    print(f"API Server starting on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)