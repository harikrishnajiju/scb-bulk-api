from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import threading
import time
import os
from datetime import datetime
import redis
import psycopg2
from contextlib import contextmanager

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://banking_user:banking_pass@localhost:5432/banking_api')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

print(f"Starting Banking Bulk API Service")
print(f"Kafka: {KAFKA_SERVERS}")
print(f"Database: {DATABASE_URL}")
print(f"Redis: {REDIS_URL}")

# Initialize connections with retry logic
def init_kafka_producer():
    retries = 5
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVERS],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("Kafka producer connected")
            return producer
        except Exception as e:
            print(f"Kafka connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Kafka")

def init_redis_client():
    retries = 5
    for i in range(retries):
        try:
            client = redis.from_url(REDIS_URL)
            client.ping()
            print("Redis connected")
            return client
        except Exception as e:
            print(f"Redis connection attempt {i+1}/{retries} failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Redis")

# Initialize connections
producer = init_kafka_producer()
redis_client = init_redis_client()

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Core Banking APIs

@app.route('/')
def index():
    """Welcome endpoint"""
    return {
        'service': 'Banking Bulk API Platform',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'bulk_extract': 'POST /api/v1/bulk/extract',
            'bulk_load': 'POST /api/v1/bulk/load',
            'streaming': 'GET /api/v1/stream/{job_id}',
            'legacy_integration': 'POST /api/v1/legacy/{program}',
            'teams': 'GET /api/v1/teams',
            'health': 'GET /health'
        },
        'description': 'Replace file-based data sharing with real-time APIs'
    }

@app.route('/api/v1/bulk/extract', methods=['POST'])
def bulk_extract():
    """
    CORE FEATURE: Replace file exports with streaming API
    
    Example: Risk team requests customer data from Operations team
    OLD: Operations exports CSV → uploads to shared drive → Risk downloads
    NEW: Risk calls API → gets real-time stream of data
    """
    try:
        request_data = request.json
        requesting_team = request.headers.get('X-Team', 'unknown-team')
        job_id = str(uuid.uuid4())
        
        print(f"Bulk extract request from {requesting_team}: {request_data}")
        
        # Validate request
        if not request_data.get('source'):
            return {'error': 'Source is required (e.g., "operations-team.customers")'}, 400
            
        # Job metadata
        job_metadata = {
            'job_id': job_id,
            'status': 'processing',
            'source': request_data['source'],
            'filters': request_data.get('filters', {}),
            'requesting_team': requesting_team,
            'created_at': datetime.utcnow().isoformat(),
            'estimated_records': request_data.get('limit', 1000)
        }
        
        # Store job info in Redis
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_metadata))
        
        # Send to Kafka for async processing
        producer.send('bulk-extract-jobs', {
            'job_id': job_id,
            'request': request_data,
            'requesting_team': requesting_team,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'job_id': job_id,
            'status': 'processing',
            'message': 'Data extraction started. Use stream_url for real-time results.',
            'stream_url': f'/api/v1/stream/{job_id}',
            'status_url': f'/api/v1/jobs/{job_id}/status',
            'estimated_records': job_metadata['estimated_records']
        }
        
    except Exception as e:
        print(f"Error in bulk_extract: {e}")
        return {'error': f'Internal error: {str(e)}'}, 500

@app.route('/api/v1/bulk/load', methods=['POST'])
def bulk_load():
    """
    CORE FEATURE: Replace file imports with streaming API
    
    Example: Analytics team loads processed data from Risk team
    OLD: Risk exports file → Analytics downloads → imports to database
    NEW: Analytics calls API → gets real-time data stream
    """
    try:
        request_data = request.json
        requesting_team = request.headers.get('X-Team', 'unknown-team')
        job_id = str(uuid.uuid4())
        
        print(f"Bulk load request from {requesting_team}: {request_data}")
        
        # Validate request
        if not request_data.get('target'):
            return {'error': 'Target is required (e.g., "analytics-team.processed_data")'}, 400
            
        job_metadata = {
            'job_id': job_id,
            'status': 'processing',
            'target': request_data['target'],
            'data_source': request_data.get('data_source'),
            'requesting_team': requesting_team,
            'created_at': datetime.utcnow().isoformat()
        }
        
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_metadata))
        
        producer.send('bulk-load-jobs', {
            'job_id': job_id,
            'request': request_data,
            'requesting_team': requesting_team,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'job_id': job_id,
            'status': 'processing',
            'message': 'Data loading started',
            'status_url': f'/api/v1/jobs/{job_id}/status'
        }
        
    except Exception as e:
        print(f"Error in bulk_load: {e}")
        return {'error': f'Internal error: {str(e)}'}, 500

@app.route('/api/v1/stream/<job_id>')
def stream_results(job_id):
    """
    CORE FEATURE: Stream results in real-time instead of downloading files
    
    This replaces the "download CSV from shared drive" step
    """
    def generate():
        try:
            print(f"Starting stream for job {job_id}")
            
            # Create consumer for this specific job
            consumer = KafkaConsumer(
                f'bulk-results-{job_id}',
                bootstrap_servers=[KAFKA_SERVERS],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=30000,  # 30 second timeout
                auto_offset_reset='earliest'
            )
            
            # Send initial connection confirmation
            yield "data: " + json.dumps({
                "status": "connected", 
                "job_id": job_id,
                "message": "Streaming data in real-time..."
            }) + "\n\n"
            
            record_count = 0
            for message in consumer:
                record_count += 1
                yield f"data: {json.dumps(message.value)}\n\n"
                
                # Check if job is complete
                if message.value.get('status') == 'complete':
                    print(f"Stream completed for job {job_id}, sent {record_count} records")
                    break
                    
        except Exception as e:
            print(f"Error in stream {job_id}: {e}")
            yield f"data: {json.dumps({'error': str(e), 'job_id': job_id})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/v1/jobs/<job_id>/status')
def job_status(job_id):
    """Get job status and metadata"""
    try:
        job_data = redis_client.get(f'job:{job_id}')
        if not job_data:
            return {'error': 'Job not found'}, 404
            
        return json.loads(job_data)
        
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/api/v1/legacy/<program>', methods=['POST'])
def legacy_integration(program):
    """
    CORE FEATURE: Modern API wrapper for legacy systems
    
    Example: Call mainframe COBOL programs via REST API
    OLD: Submit batch file → overnight processing → pick up result file
    NEW: Real-time API call → immediate JSON response
    """
    try:
        request_data = request.json
        requesting_team = request.headers.get('X-Team', 'unknown-team')
        job_id = str(uuid.uuid4())
        
        print(f"Legacy system call: {program} from {requesting_team}")
        print(f"Parameters: {request_data}")
        
        # Simulate legacy system integration
        result = simulate_legacy_call(program, request_data)
        
        return {
            'job_id': job_id,
            'program': program,
            'requesting_team': requesting_team,
            'result': result,
            'timestamp': datetime.utcnow().isoformat(),
            'message': f'Legacy program {program} executed successfully'
        }
        
    except Exception as e:
        print(f"Error in legacy call {program}: {e}")
        return {'error': str(e)}, 500

def simulate_legacy_call(program, params):
    """
    Simulate legacy system integration
    In production: replace with actual connectors (MQ, SOAP, socket, etc.)
    """
    # Simulate processing time
    time.sleep(0.5)
    
    # Mock responses for different banking programs
    if program == 'CUSTBAL':
        return {
            'account': params.get('account', '12345'),
            'balance': 47500.00,
            'available_balance': 45000.00,
            'currency': 'USD',
            'account_type': 'SAVINGS',
            'last_transaction_date': '2025-06-30',
            'status': 'ACTIVE'
        }
    elif program == 'TXNHIST':
        return {
            'account': params.get('account', '12345'),
            'transaction_count': 25,
            'transactions': [
                {
                    'date': '2025-06-30T14:30:00Z',
                    'amount': -150.00,
                    'type': 'DEBIT',
                    'description': 'ATM Withdrawal - Main Street',
                    'balance_after': 47500.00
                },
                {
                    'date': '2025-06-30T09:15:00Z', 
                    'amount': 2800.00,
                    'type': 'CREDIT',
                    'description': 'Salary Deposit - ACME Corp',
                    'balance_after': 47650.00
                },
                {
                    'date': '2025-06-29T16:45:00Z',
                    'amount': -75.50,
                    'type': 'DEBIT', 
                    'description': 'Online Purchase - Amazon',
                    'balance_after': 44850.00
                }
            ],
            'page': 1,
            'total_pages': 3
        }
    elif program == 'RISKSCORE':
        return {
            'customer_id': params.get('customer_id', '12345'),
            'risk_score': 7.2,
            'risk_category': 'MEDIUM',
            'factors': [
                'Multiple international transfers',
                'High transaction velocity', 
                'New account relationship'
            ],
            'last_updated': datetime.utcnow().isoformat(),
            'next_review_date': '2025-07-30'
        }
    else:
        return {
            'program': program,
            'status': 'SUCCESS',
            'message': f'Program {program} executed successfully',
            'parameters': params,
            'execution_time': '0.5s'
        }

@app.route('/api/v1/teams', methods=['GET'])
def list_teams():
    """
    List registered teams and their data sharing capabilities
    """
    teams = {
        'risk-team': {
            'description': 'Risk Management and Fraud Detection',
            'apis': ['/api/v1/risk/*'],
            'data_sources': [
                'customer_risk_scores',
                'fraud_alerts',
                'compliance_reports'
            ],
            'data_consumers': ['operations-team', 'compliance-team'],
            'contact': 'risk-team@bank.com'
        },
        'operations-team': {
            'description': 'Core Banking Operations',
            'apis': ['/api/v1/transactions/*', '/api/v1/accounts/*'],
            'data_sources': [
                'transaction_logs',
                'account_balances', 
                'customer_profiles'
            ],
            'data_consumers': ['risk-team', 'analytics-team', 'compliance-team'],
            'contact': 'operations@bank.com'
        },
        'compliance-team': {
            'description': 'Regulatory Compliance and Audit',
            'apis': ['/api/v1/compliance/*'],
            'data_sources': [
                'audit_logs',
                'regulatory_reports',
                'kyc_data'
            ],
            'data_consumers': ['risk-team', 'management-team'],
            'contact': 'compliance@bank.com'
        },
        'analytics-team': {
            'description': 'Business Intelligence and Analytics',
            'apis': ['/api/v1/analytics/*'],
            'data_sources': [
                'customer_insights',
                'performance_metrics',
                'predictive_models'
            ],
            'data_consumers': ['management-team', 'marketing-team'],
            'contact': 'analytics@bank.com'
        }
    }
    return {
        'teams': teams,
        'total_teams': len(teams),
        'message': 'Available teams for data sharing via bulk APIs'
    }

@app.route('/health')
def health_check():
    """
    Comprehensive health check for monitoring
    """
    try:
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'version': '1.0.0',
            'services': {}
        }
        
        # Check Kafka
        try:
            producer.send('health-check', {
                'timestamp': datetime.utcnow().isoformat(),
                'service': 'banking-api'
            })
            health_status['services']['kafka'] = 'connected'
        except Exception as e:
            health_status['services']['kafka'] = f'error: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check Redis
        try:
            redis_client.ping()
            health_status['services']['redis'] = 'connected'
        except Exception as e:
            health_status['services']['redis'] = f'error: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check Database
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1')
                cursor.fetchone()
            health_status['services']['database'] = 'connected'
        except Exception as e:
            health_status['services']['database'] = f'error: {str(e)}'
            health_status['status'] = 'degraded'
        
        status_code = 200 if health_status['status'] == 'healthy' else 503
        return health_status, status_code
        
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }, 500

# Background job processor
def process_bulk_jobs():
    """
    Background worker to process bulk extract and load jobs
    """
    print("Starting background job processor...")
    
    try:
        consumer = KafkaConsumer(
            'bulk-extract-jobs',
            'bulk-load-jobs',
            bootstrap_servers=[KAFKA_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='banking-job-processor'
        )
        
        for message in consumer:
            try:
                job_data = message.value
                job_id = job_data['job_id']
                
                print(f"Processing job {job_id} from topic {message.topic}")
                
                if message.topic == 'bulk-extract-jobs':
                    process_extract_job(job_id, job_data)
                elif message.topic == 'bulk-load-jobs':
                    process_load_job(job_id, job_data)
                    
            except Exception as e:
                print(f"Error processing job: {e}")
                
    except Exception as e:
        print(f"Error in job processor: {e}")

def process_extract_job(job_id, job_data):
    """
    Process data extraction job - simulates pulling data from source systems
    """
    try:
        request = job_data['request']
        source = request['source']
        filters = request.get('filters', {})
        limit = request.get('limit', 100)
        
        print(f"Extracting data from {source} with filters: {filters}")
        
        # Simulate data extraction with realistic banking data
        for i in range(min(limit, 100)):  # Limit to 100 for demo
            if source.endswith('customers'):
                record = generate_customer_record(i, filters)
            elif source.endswith('transactions'):
                record = generate_transaction_record(i, filters)
            else:
                record = generate_generic_record(i, source, filters)
            
            # Send record to results topic
            producer.send(f'bulk-results-{job_id}', record)
            time.sleep(0.02)  # Simulate processing time
        
        # Send completion message
        completion_message = {
            'status': 'complete',
            'total_records': min(limit, 100),
            'job_id': job_id,
            'source': source,
            'completed_at': datetime.utcnow().isoformat()
        }
        
        producer.send(f'bulk-results-{job_id}', completion_message)
        
        # Update job status in Redis
        job_metadata = json.loads(redis_client.get(f'job:{job_id}'))
        job_metadata['status'] = 'completed'
        job_metadata['completed_at'] = datetime.utcnow().isoformat()
        job_metadata['total_records'] = min(limit, 100)
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_metadata))
        
        print(f"Extract job {job_id} completed, {min(limit, 100)} records processed")
        
    except Exception as e:
        print(f"Error in extract job {job_id}: {e}")

def generate_customer_record(index, filters):
    """Generate realistic customer data"""
    regions = ['APAC', 'EMEA', 'AMERICAS']
    account_types = ['SAVINGS', 'CHECKING', 'PREMIUM', 'CORPORATE']
    
    return {
        'customer_id': f'CUST_{10000 + index}',
        'account_number': f'ACC_{50000 + index}',
        'name': f'Customer Name {index}',
        'account_type': account_types[index % len(account_types)],
        'balance': round(1000 + (index * 157.33), 2),
        'region': regions[index % len(regions)],
        'created_date': '2025-01-15T10:30:00Z',
        'status': 'ACTIVE',
        'risk_score': round(1 + (index % 10), 1),
        'last_activity': datetime.utcnow().isoformat()
    }

def generate_transaction_record(index, filters):
    """Generate realistic transaction data"""
    transaction_types = ['DEBIT', 'CREDIT', 'TRANSFER', 'FEE']
    channels = ['ATM', 'ONLINE', 'MOBILE', 'BRANCH', 'POS']
    
    return {
        'transaction_id': f'TXN_{100000 + index}',
        'account_number': f'ACC_{50000 + (index % 100)}',
        'amount': round(-500 + (index * 23.45), 2),
        'type': transaction_types[index % len(transaction_types)],
        'channel': channels[index % len(channels)],
        'description': f'Transaction {index} via {channels[index % len(channels)]}',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'COMPLETED',
        'merchant': f'Merchant_{index % 50}' if index % 4 == 0 else None
    }

def generate_generic_record(index, source, filters):
    """Generate generic data record"""
    return {
        'record_id': f'REC_{index}',
        'source': source,
        'data': f'Sample data record {index}',
        'filters_applied': filters,
        'timestamp': datetime.utcnow().isoformat(),
        'processed_by': 'banking-bulk-api'
    }

def process_load_job(job_id, job_data):
    """
    Process data loading job - simulates loading data to target systems
    """
    try:
        request = job_data['request']
        target = request['target']
        
        print(f"Loading data to {target}")
        
        # Simulate data loading processing
        time.sleep(1)
        
        # Update job status
        job_metadata = json.loads(redis_client.get(f'job:{job_id}'))
        job_metadata['status'] = 'completed'
        job_metadata['completed_at'] = datetime.utcnow().isoformat()
        redis_client.setex(f'job:{job_id}', 3600, json.dumps(job_metadata))
        
        print(f"Load job {job_id} completed for target: {target}")
        
    except Exception as e:
        print(f"Error in load job {job_id}: {e}")

# Start the application
if __name__ == '__main__':
    print("Banking Bulk API Platform Starting...")
    print("Features:")
    print("   • Replace file transfers with real-time APIs")
    print("   • Stream data instead of downloading files")
    print("   • Modern wrapper for legacy systems")
    print("   • Team-to-team data sharing")
    
    # Start background job processor
    job_processor_thread = threading.Thread(target=process_bulk_jobs, daemon=True)
    job_processor_thread.start()
    print("Background job processor started")
    
    print("API Server starting on http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)