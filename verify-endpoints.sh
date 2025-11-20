#!/bin/bash

echo "================================"
echo "Carbon Profiling System Checker"
echo "================================"
echo ""

# Get the API endpoint
echo "📡 Getting Ingestion API endpoint..."
API_URL=$(minikube service ingestion-api-service -n carbon-profiling --url 2>/dev/null | head -n 1)
echo "API URL: $API_URL"
echo ""

# Check API health
echo "🏥 Checking API health..."
curl -s "$API_URL/health" | python3 -m json.tool
echo ""

# Check stats
echo "📊 Checking system stats..."
STATS=$(curl -s "$API_URL/api/v1/stats")
echo "$STATS" | python3 -m json.tool
echo ""

# Parse total records
TOTAL_RECORDS=$(echo "$STATS" | python3 -c "import sys, json; print(json.load(sys.stdin).get('total_records', 0))" 2>/dev/null || echo "0")

if [ "$TOTAL_RECORDS" -eq 0 ]; then
    echo "⚠️  NO DATA FOUND!"
    echo ""
    echo "To start collecting data, run the device agent:"
    echo ""
    echo "  cd device-agent"
    echo "  export API_ENDPOINT=\"$API_URL\""
    echo "  export SEND_TO_API=true"
    echo "  python device_agent.py"
    echo ""
else
    echo "✅ Found $TOTAL_RECORDS records"
    echo ""
    
    # Check carbon data
    echo "💨 Checking carbon footprint data..."
    curl -s "$API_URL/api/v1/carbon/summary" | python3 -m json.tool
    echo ""
fi

# Check pod status
echo "🔍 Pod Status:"
kubectl get pods -n carbon-profiling
echo ""

# Check worker logs (last 10 lines)
echo "📝 Recent Worker Logs:"
kubectl logs --tail=10 deployment/carbon-profiler -n carbon-profiling 2>/dev/null || echo "Worker not found or not running"
echo ""

# Get dashboard URL
echo "🎨 Dashboard URL:"
minikube service dashboard-service -n carbon-profiling --url 2>/dev/null | head -n 1
echo ""

echo "================================"
echo "Environment variables for device agent:"
echo "================================"
echo "export API_ENDPOINT=\"$API_URL\""
echo "export SEND_TO_API=true"
echo ""


