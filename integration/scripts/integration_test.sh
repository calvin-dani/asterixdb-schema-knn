#!/bin/bash
# Integration test script for movie_index_test.py
# Tests both standard (non-quantized) and quantized vector indexes

set -e  # Exit on error

DATASET="movie_embeddings_384d.json"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "Integration Test Suite for Vector Index"
echo "============================================================"
echo ""

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_section() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Test 1: Standard Index (No Quantization) - Full Workflow
print_section "TEST 1: Standard Index - Load Initial Dataset (3000 records)"
print_info "Creating standard index without quantization"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 --no-query --limit 3000
print_success "Test 1 completed"

# Test 2: Insert records [3000, 5999]
print_section "TEST 2: Standard Index - Insert Records [3000, 5999]"
print_info "Inserting additional 3000 records"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --insert 3000 5999
print_success "Test 2 completed"

# Test 3: Delete records [2000, 3999]
print_section "TEST 3: Standard Index - Delete Records [2000, 3999]"
print_info "Deleting 2000 records from middle range"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --delete 2000 3999
print_success "Test 3 completed"

# Test 4: Verify query results without year filter
print_section "TEST 4: Standard Index - Verify Query (No Year Filter)"
print_info "Verifying ANN query returns expected records from intervals [0, 1999] and [4000, 5999]"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --verify 400 0 1999 4000 5999
print_success "Test 4 completed"

# Test 5: Verify query with year filter
print_section "TEST 5: Standard Index - Verify Query (With Year Filter)"
print_info "Verifying ANN query with year > 2000 filter"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --verify 50 0 1999 4000 5999 --year 2000
print_success "Test 5 completed"

# Test 6: Quantized Index - Load Initial Dataset (3000 records)
print_section "TEST 6: Quantized Index (SQ7) - Load Initial Dataset (3000 records)"
print_info "Creating quantized index with SQ7, euclidean-squared similarity"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 --no-query \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000
print_success "Test 6 completed"

# Test 7: Insert records [3000, 5999] (quantized)
print_section "TEST 7: Quantized Index - Insert Records [3000, 5999]"
print_info "Inserting additional 3000 records into quantized index"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --insert 3000 5999
print_success "Test 7 completed"

# Test 8: Delete records [2000, 3999] (quantized)
print_section "TEST 8: Quantized Index - Delete Records [2000, 3999]"
print_info "Deleting 2000 records from quantized index"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --delete 2000 3999
print_success "Test 8 completed"

# Test 9: Verify query results without year filter (quantized)
print_section "TEST 9: Quantized Index - Verify Query (No Year Filter)"
print_info "Verifying ANN query on quantized index"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET --verify 400 0 1999 4000 5999
print_success "Test 9 completed"

# Test 10: Full workflow test with standard index
print_section "TEST 10: Standard Index - Full Workflow with Query"
print_info "Running full workflow: dataset + index + query with year filter"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 --limit 3000
print_success "Test 10 completed"

# Test 11: Full workflow test with quantized index (SQ8)
print_section "TEST 11: Quantized Index (SQ8) - Full Workflow with Query"
print_info "Running full workflow with SQ8 quantization"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 \
    --limit 3000 \
    --quantization SQ8 \
    --similarity Euclidean \
    --train-list 3000
print_success "Test 11 completed"

# Test 12: Test with INCLUDE field and quantization
print_section "TEST 12: Quantized Index with INCLUDE Field"
print_info "Creating quantized index with INCLUDE(year) clause"
python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 \
    --limit 3000 \
    --quantization SQ7 \
    --similarity euclidean-squared \
    --train-list 3000 \
    --include-field year
print_success "Test 12 completed"

# Summary
print_section "INTEGRATION TEST SUITE COMPLETED"
echo -e "${GREEN}All tests passed successfully!${NC}"
echo ""
echo "Tests completed:"
echo "  1. Standard index creation and data loading"
echo "  2. Insert operations (standard index)"
echo "  3. Delete operations (standard index)"
echo "  4. Query verification without year filter (standard)"
echo "  5. Query verification with year filter (standard)"
echo "  6. Quantized index creation (SQ7)"
echo "  7. Insert operations (quantized index)"
echo "  8. Delete operations (quantized index)"
echo "  9. Query verification (quantized index)"
echo "  10. Full workflow (standard index)"
echo "  11. Full workflow with quantization (SQ8)"
echo "  12. Quantized index with INCLUDE field"
echo ""
echo -e "${GREEN}✓ All integration tests completed successfully!${NC}"
