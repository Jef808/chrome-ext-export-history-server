# Usage example

``` sh
curl -X POST "http://localhost:8000/events" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "page_visit",
    "url": "https://example.com",
    "tabId": 123,
    "timestamp": "2024-01-15T10:30:00Z"
  }'
```
