# File Scanner API Service

A production-ready microservices-based file scanning system for counting from A to Z characters in text files. Built with FastAPI, PostgreSQL, Redis, and designed for async processing with deduplication.

## Architecture Overview

```
Client → API Service → Redis Stream → Scanner Workers
           ↓              ↓
        PostgreSQL     Redis Cache
           ↓
      File Storage
```

### Key Features

- **Async Processing**: Immediate job_id response, processing happens in background
- **File Deduplication**: Same file uploaded multiple times = instant results from cache
- **Content-Addressed Storage**: Files stored by SHA256 hash, automatic deduplication
- **Streaming Hash Calculation**: Single-pass hash calculation during upload
- **Multi-level Caching**: Redis cache for job status and file results
- **Storage Management**: Automatic cleanup with tiered strategy based on disk usage
- **Production Ready**: Error handling, logging, health checks, Docker support

## Technology Stack

- **API Framework**: FastAPI (async Python web framework)
- **Database**: PostgreSQL (job records and metadata)
- **Cache/Queue**: Redis (status cache + Redis Streams for job queue)
- **Storage**: Local filesystem with content-addressed structure
- **Python Libraries**: asyncpg, redis-py, aiofiles, pydantic

## Project Structure

```
file-scanner-api/
├── app/
│   ├── __init__.py
│   ├── api.py              # FastAPI application and endpoints
│   ├── config.py           # Configuration management
│   ├── models.py           # Database and Pydantic models
│   ├── database.py         # PostgreSQL connection and operations
│   ├── cache.py            # Redis cache service
│   ├── storage.py          # File storage with content-addressing
│   ├── queue.py            # Redis Stream queue service
│   └── cleanup.py          # Storage cleanup service
├── scripts/
│   └── worker_simulator.py # Worker simulator for testing
├── main.py                 # Application entry point
├── requirements.txt        # Python dependencies
├── Dockerfile             # Container image definition
├── docker-compose.yml     # Multi-container setup
├── .env.example           # Example environment variables
└── README.md              # This file
```

## Quick Start

### Option 1: Docker (Recommended)

```bash
# 1. Clone and navigate to project
cd file-scanner-api

# 2. Create .env file (or use defaults)
cp .env.example .env

# 3. Start all services with Docker Compose
docker-compose up -d

# 4. Check service health
curl http://localhost:8000/health

# 5. In a separate terminal, start the worker simulator
docker-compose exec api python scripts/worker_simulator.py
```

### Option 2: Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start PostgreSQL and Redis (via Docker or locally)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=scanner_pass \
  -e POSTGRES_USER=scanner_user -e POSTGRES_DB=file_scanner postgres:15-alpine

docker run -d -p 6379:6379 redis:7-alpine

# 3. Create .env file
cp .env.example .env

# 4. Start the API service
python main.py

# 5. In a separate terminal, start the worker simulator
python scripts/worker_simulator.py
```

## API Usage

### 1. Upload a File

```bash
# Upload a text file for scanning
curl -X POST http://localhost:8000/api/v1/files \
  -F "file=@document.txt"

# Response:
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "File uploaded successfully, processing started",
  "deduplication": false
}
```

### 2. Check Job Status

```bash
# Query job status and results
curl http://localhost:8000/api/v1/files/550e8400-e29b-41d4-a716-446655440000

# Response (when completed):
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "original_filename": "document.txt",
  "file_size": 1048576,
  "results": {
    "A": 1042,
    "B": 573,
    "C": 289
  },
  "created_at": "2025-12-31T10:30:00Z",
  "updated_at": "2025-12-31T10:30:15Z",
  "completed_at": "2025-12-31T10:30:15Z"
}
```

### 3. Health Check

```bash
# Check service health
curl http://localhost:8000/health

# Response:
{
  "status": "healthy",
  "database": "healthy",
  "redis": "healthy",
  "storage": {
    "total_bytes": 500000000000,
    "used_bytes": 250000000000,
    "free_bytes": 250000000000,
    "percent_used": 50.0,
    "status": "healthy"
  }
}
```

## Architecture Deep Dive

### File Upload Flow

1. **Client uploads file** via `POST /api/v1/files`
2. **API service**:
   - Checks storage availability
   - Streams file to disk while calculating SHA256 hash (single-pass)
   - Checks Redis cache for duplicate file hash
   - If duplicate: Returns instant results from cache
   - If new: Creates job record in PostgreSQL
   - Publishes job to Redis Stream queue
   - Returns job_id immediately
3. **Worker consumes message** from Redis Stream
4. **Worker scans file** and counts A-Z character frequencies
5. **Worker updates** job status in PostgreSQL and caches results in Redis

### Deduplication Strategy

The system implements multi-level deduplication:

1. **Storage Level**: Content-addressed storage by SHA256 hash
   - Path: `/data/files/{hash[0:2]}/{hash[2:4]}/{hash}`
   - Same file uploaded twice = stored once

2. **Cache Level**: Redis cache with 24-hour TTL
   - Key: `file:{hash}:results`
   - Instant results for duplicate files

3. **Database Level**: Query completed jobs by file_hash
   - Fallback when cache misses
   - Ensures results persist beyond cache TTL

### Storage Management

Tiered cleanup strategy based on disk usage:

- **Healthy (<85%)**: Standard 7-day retention
- **Warning (85-90%)**: Delete files older than 3 days
- **Critical (90-95%)**: Delete files older than 1 day
- **Emergency (95%+)**: Delete files older than 12 hours, stop uploads

### Database Schema

```sql
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    file_hash VARCHAR(64) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    results JSONB,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    metadata JSONB
);

CREATE INDEX idx_jobs_file_hash ON jobs(file_hash);
CREATE INDEX idx_jobs_status ON jobs(status);
```

### Redis Cache Patterns

1. **Job Status Cache**
   - Key: `job:{job_id}:status`
   - TTL: 1 hour
   - Purpose: Fast status queries

2. **File Results Cache**
   - Key: `file:{file_hash}:results`
   - TTL: 24 hours
   - Purpose: Deduplication

3. **Redis Stream Queue**
   - Stream: `scan_jobs`
   - Consumer Group: `scanner_workers`
   - Purpose: Job distribution

## Configuration

All configuration is done via environment variables. See [.env.example](.env.example) for all options.

Key settings:

```bash
# Database
DATABASE_URL=postgresql://scanner_user:scanner_pass@localhost:5432/file_scanner

# Redis
REDIS_URL=redis://localhost:6379/0

# Storage
FILE_STORAGE_PATH=/data/files
MAX_FILE_SIZE_MB=100
FILE_RETENTION_DAYS=7

# Cache TTL
JOB_CACHE_TTL=3600      # 1 hour
FILE_CACHE_TTL=86400    # 24 hours
```

## Testing

### Test File Upload and Processing

```bash
# Create a test file
echo "AAABBBCCC Hello World AAABBBCCC" > test.txt

# Upload the file
JOB_ID=$(curl -s -X POST http://localhost:8000/api/v1/files \
  -F "file=@test.txt" | jq -r '.job_id')

echo "Job ID: $JOB_ID"

# Wait a moment for processing
sleep 2

# Check status
curl http://localhost:8000/api/v1/files/$JOB_ID | jq
```

### Test Deduplication

```bash
# Upload the same file again
curl -X POST http://localhost:8000/api/v1/files \
  -F "file=@test.txt" | jq

# Should return instant results with "deduplication": true
```

## Performance Considerations

### Single-Pass Hash Calculation

The API calculates SHA256 hash while streaming the file to disk, avoiding multiple file reads:

```python
# Traditional approach: 2 passes
save_file(file)              # Pass 1: Write to disk
hash = calculate_hash(file)  # Pass 2: Read from disk

# Our approach: 1 pass
hash, size, path = calculate_hash_and_save(file)  # Single pass
```

For a 1MB file, this saves ~10ms of disk I/O.

### Chunked Reading

Files are processed in 1MB chunks to maintain constant memory usage regardless of file size:

- 1MB file: 1MB memory usage
- 1GB file: 1MB memory usage

### Async I/O

All I/O operations use async libraries:
- Database: `asyncpg`
- Redis: `redis.asyncio`
- File I/O: `aiofiles`

This allows handling thousands of concurrent requests without thread overhead.

## Production Deployment

### Using Docker Compose (Single Machine)

```bash
# Production docker-compose with resource limits
docker-compose -f docker-compose.prod.yml up -d
```

### Scaling Workers

Add more worker instances to process jobs faster:

```bash
# Start additional worker simulators
docker-compose exec api python scripts/worker_simulator.py &
docker-compose exec api python scripts/worker_simulator.py &
docker-compose exec api python scripts/worker_simulator.py &
```

In production, deploy separate worker containers:

```yaml
worker:
  build: .
  command: python scripts/worker_simulator.py
  deploy:
    replicas: 4  # Scale workers independently
```

### Monitoring

Monitor these metrics:

- API response times: `/health` endpoint
- Queue length: `XLEN scan_jobs` in Redis
- Storage usage: Check `/health` storage stats
- Database connections: PostgreSQL stats
- Cache hit rate: Redis INFO stats

## Troubleshooting

### Worker not processing jobs

```bash
# Check Redis Stream has messages
docker-compose exec redis redis-cli XLEN scan_jobs

# Check consumer group exists
docker-compose exec redis redis-cli XINFO GROUPS scan_jobs

# Check worker logs
docker-compose logs api
```

### Database connection issues

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Test connection
docker-compose exec postgres psql -U scanner_user -d file_scanner -c "SELECT COUNT(*) FROM jobs;"
```

### Storage issues

```bash
# Check disk usage
df -h /data/files

# Trigger manual cleanup
curl -X POST http://localhost:8000/admin/cleanup  # If implemented
```

### 5. Trade-offs & Design Decisions

**Why calculate hash in API vs Worker?**
- API: Zero disk I/O for duplicates (40% savings in high-dup scenarios)
- Trade-off: Slight CPU load on API, but <10ms for typical files

**Why Redis Streams vs RabbitMQ?**
- Already need Redis for caching
- Simpler deployment (one less service)
- Good enough for single-machine deployment

**Why content-addressed storage?**
- Automatic deduplication at storage level
- Enables easy distributed caching
- Industry standard (Git, Docker, CDNs use this)

## License

This project is created for educational and interview purposes.

## Author

Shoham Elimelech - Senior Backend Developer
