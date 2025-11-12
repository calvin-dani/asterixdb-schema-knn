#!/bin/bash

# Script to analyze cluster load distribution from VCTree bulk loading logs
# Usage: ./analyze_cluster_load.sh <log_file> [output_file]

set -e

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <log_file> [output_file]"
    echo "Example: $0 log3 cluster_analysis.txt"
    exit 1
fi

LOG_FILE="$1"
OUTPUT_FILE="${2:-cluster_load_records.txt}"

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file '$LOG_FILE' not found"
    exit 1
fi

echo "=================================================="
echo "VCTree Cluster Load Analysis"
echo "=================================================="
echo "Log file: $LOG_FILE"
echo "Output file: $OUTPUT_FILE"
echo ""

# Extract cluster load records
echo "Extracting cluster load records..."
grep "===== record #" "$LOG_FILE" > "$OUTPUT_FILE"
TOTAL_RECORDS=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
echo "Total records extracted: $TOTAL_RECORDS"
echo ""

# Show first 10 records
echo "=================================================="
echo "First 10 Records:"
echo "=================================================="
head -10 "$OUTPUT_FILE"
echo ""

# Show last 10 records
echo "=================================================="
echo "Last 10 Records:"
echo "=================================================="
tail -10 "$OUTPUT_FILE"
echo ""

# Analyze cluster distribution
echo "=================================================="
echo "Cluster Distribution Analysis:"
echo "=================================================="

# Extract cluster numbers and count
CLUSTER_STATS=$(awk -F'cluster #' '{print $2}' "$OUTPUT_FILE" | awk '{print $1}' | sort -n | uniq -c | sort -k2 -n)

echo "Cluster | Count  | Percentage | Bar Chart"
echo "--------|--------|------------|--------------------------------------------"

# Calculate statistics
MAX_COUNT=0
MIN_COUNT=999999999
TOTAL_CLUSTERS=0

while read -r count cluster; do
    if [ "$count" -gt "$MAX_COUNT" ]; then
        MAX_COUNT=$count
    fi
    if [ "$count" -lt "$MIN_COUNT" ]; then
        MIN_COUNT=$count
    fi
    TOTAL_CLUSTERS=$((TOTAL_CLUSTERS + 1))
done <<< "$CLUSTER_STATS"

# Print distribution with bar chart
while read -r count cluster; do
    percentage=$(echo "scale=1; ($count * 100) / $TOTAL_RECORDS" | bc)
    bar_length=$(echo "scale=0; ($count * 40) / $MAX_COUNT" | bc)
    bar=$(printf '█%.0s' $(seq 1 $bar_length 2>/dev/null))
    printf "%7s | %6s | %9s%% | %s\n" "$cluster" "$count" "$percentage" "$bar"
done <<< "$CLUSTER_STATS"

echo ""
echo "=================================================="
echo "Summary Statistics:"
echo "=================================================="
AVG=$(echo "scale=1; $TOTAL_RECORDS / $TOTAL_CLUSTERS" | bc)
IMBALANCE=$(echo "scale=1; $MAX_COUNT / $MIN_COUNT" | bc)

echo "Total clusters: $TOTAL_CLUSTERS"
echo "Total records: $TOTAL_RECORDS"
echo "Average records per cluster: $AVG"
echo "Max records in a cluster: $MAX_COUNT"
echo "Min records in a cluster: $MIN_COUNT"
echo "Imbalance ratio (max/min): ${IMBALANCE}x"
echo ""

# Calculate standard deviation
SUM_SQ_DIFF=0
while read -r count cluster; do
    diff=$(echo "$count - $AVG" | bc)
    sq_diff=$(echo "$diff * $diff" | bc)
    SUM_SQ_DIFF=$(echo "$SUM_SQ_DIFF + $sq_diff" | bc)
done <<< "$CLUSTER_STATS"

VARIANCE=$(echo "scale=2; $SUM_SQ_DIFF / $TOTAL_CLUSTERS" | bc)
STDDEV=$(echo "scale=2; sqrt($VARIANCE)" | bc)
CV=$(echo "scale=2; ($STDDEV * 100) / $AVG" | bc)

echo "Standard deviation: $STDDEV"
echo "Coefficient of variation: ${CV}%"
echo ""

# Identify problematic clusters
echo "=================================================="
echo "Potential Issues:"
echo "=================================================="

# Find clusters significantly below average
THRESHOLD=$(echo "scale=0; $AVG * 0.3 / 1" | bc)
echo "Clusters with < 30% of average ($THRESHOLD records):"
while read -r count cluster; do
    if [ "$count" -lt "$THRESHOLD" ]; then
        pct=$(echo "scale=1; ($count * 100) / $AVG" | bc)
        echo "  Cluster #$cluster: $count records (${pct}% of avg)"
    fi
done <<< "$CLUSTER_STATS"

echo ""

# Find clusters significantly above average
THRESHOLD_HIGH=$(echo "scale=0; $AVG * 2 / 1" | bc)
echo "Clusters with > 200% of average ($THRESHOLD_HIGH records):"
while read -r count cluster; do
    if [ "$count" -gt "$THRESHOLD_HIGH" ]; then
        pct=$(echo "scale=1; ($count * 100) / $AVG" | bc)
        echo "  Cluster #$cluster: $count records (${pct}% of avg)"
    fi
done <<< "$CLUSTER_STATS"

echo ""
echo "=================================================="
echo "Analysis complete!"
echo "Detailed records saved to: $OUTPUT_FILE"
echo "=================================================="