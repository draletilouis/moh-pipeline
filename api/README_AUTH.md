# API Authentication Guide

## Overview

The Uganda Health API uses **API Key Authentication** to secure access to health data endpoints. All data endpoints require a valid API key to be included in the request headers.

## Setup

### 1. Configure API Key

The API key is stored in the `conf/.env` file:

```bash
API_KEY=gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto
```

**IMPORTANT:** Change this key in production! Generate a new secure key with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 2. Public vs Protected Endpoints

**Public Endpoints** (no authentication required):
- `GET /` - Root endpoint
- `GET /test` - Test endpoint
- `GET /health` - Health check

**Protected Endpoints** (require API key):
- `GET /health/indicators` - Get health indicators
- `GET /health/indicators/metadata` - Get indicator metadata
- `GET /health/indicators/{name}/timeseries` - Get time series data
- `GET /health/rankings/top-performers` - Get rankings
- `GET /health/stats` - Get statistics
- `GET /health/quality/dashboard` - Quality dashboard
- All `/observability/*` endpoints

## Usage

### Python (requests)

```python
import requests

API_KEY = "gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto"
BASE_URL = "http://localhost:8000"

headers = {
    "X-API-Key": API_KEY
}

# Get health indicators
response = requests.get(
    f"{BASE_URL}/health/indicators",
    headers=headers,
    params={"limit": 10}
)

print(response.json())
```

### curl

```bash
curl -H "X-API-Key: gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto" \
  http://localhost:8000/health/indicators?limit=10
```

### JavaScript (fetch)

```javascript
const API_KEY = "gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto";
const BASE_URL = "http://localhost:8000";

fetch(`${BASE_URL}/health/indicators?limit=10`, {
  headers: {
    "X-API-Key": API_KEY
  }
})
  .then(response => response.json())
  .then(data => console.log(data));
```

### Power BI / Tableau

When connecting from BI tools:

1. Use the API endpoint URL
2. Add custom header: `X-API-Key`
3. Set value to your API key

**Power BI Example:**
- Data Source: Web
- URL: `http://localhost:8000/health/indicators`
- Advanced → HTTP request header parameters
  - Key: `X-API-Key`
  - Value: `gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto`

## Error Responses

### 401 Unauthorized - Missing API Key

```json
{
  "detail": "Not authenticated"
}
```

**Solution:** Include the `X-API-Key` header in your request.

### 401 Unauthorized - Invalid API Key

```json
{
  "detail": "Invalid API Key"
}
```

**Solution:** Verify your API key matches the one in `conf/.env`.

### 500 Internal Server Error - API Key Not Configured

```json
{
  "detail": "API_KEY not set in environment. Please set API_KEY in conf/.env file"
}
```

**Solution:** Add `API_KEY=your-key-here` to `conf/.env`.

## Testing Authentication

### Test Public Endpoint (No Auth Required)

```bash
curl http://localhost:8000/test
```

Should return:
```json
{
  "message": "API is working!",
  "timestamp": "2025-11-15T12:00:00",
  "endpoints": [...]
}
```

### Test Protected Endpoint Without API Key

```bash
curl http://localhost:8000/health/indicators
```

Should return `401 Unauthorized`.

### Test Protected Endpoint With API Key

```bash
curl -H "X-API-Key: gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto" \
  http://localhost:8000/health/indicators?limit=5
```

Should return health indicator data.

## CORS Configuration

For production deployments, update `ALLOWED_ORIGINS` in `conf/.env`:

```bash
# Development (allow all)
ALLOWED_ORIGINS=*

# Production (specific domains only)
ALLOWED_ORIGINS=https://dashboard.health.go.ug,https://analytics.health.go.ug
```

## Security Best Practices

1. **Never commit API keys to version control**
   - `.env` is in `.gitignore`
   - Use `.env.example` as a template only

2. **Use HTTPS in production**
   - API keys transmitted over HTTP can be intercepted
   - Always use SSL/TLS certificates

3. **Rotate keys regularly**
   - Generate new keys periodically
   - Update all clients when rotating

4. **Use different keys per environment**
   - Development, staging, and production should have unique keys

5. **Monitor API usage**
   - Track which clients are using your API
   - Set up alerts for suspicious activity

## Upgrading to Database-Backed Keys

For production with multiple clients, consider upgrading to database-backed API keys:

- Track multiple API keys per client
- Implement rate limiting
- Add audit logging
- Enable key expiration
- Add key revocation

See the full implementation guide in the project documentation.

## Interactive API Documentation

Visit the auto-generated API docs to test endpoints interactively:

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

To use authentication in Swagger UI:
1. Click the "Authorize" button (🔓)
2. Enter your API key: `gvpom8MmQI2lvT1ms9b_Ll0XAtn9tm-uAtGdgBUNBto`
3. Click "Authorize"
4. Test endpoints directly from the browser

## Support

For issues or questions:
- Check error responses above
- Review the main README.md
- Contact the development team
