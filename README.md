# Chrome History Export Server
This is a simple http server to serve as backend for [this chrome extension](https://github.com/Jef808/chrome-ext-export-history).

It receives navigation and tab-switch events and store them in a sqlite database.

## Usage example

``` sh
curl -X POST "http://localhost:8000/events" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "navigation",
    "url": "https://example.com",
    "tabId": 123,
    "timestamp": "2024-01-15T10:30:00Z",
    "user": "email@example.com" 
  }'
```
