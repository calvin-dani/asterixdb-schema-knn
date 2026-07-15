#!/bin/bash
# Quick test script for common scenarios

DATASET="movie_embeddings_384d.json"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==================================="
echo "Quick Test Options"
echo "==================================="
echo "1. Test standard index (no quantization)"
echo "2. Test quantized index (SQ7)"
echo "3. Test quantized index (SQ8)"
echo "4. Test insert/delete/verify (standard)"
echo "5. Test insert/delete/verify (quantized SQ7)"
echo ""

# Default to option 1 if no argument provided
OPTION=${1:-1}

case $OPTION in
    1)
        echo "Running: Standard Index Test"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 --limit 3000
        ;;
    2)
        echo "Running: Quantized Index Test (SQ7)"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 \
            --limit 3000 \
            --quantization SQ7 \
            --similarity euclidean-squared \
            --train-list 3000
        ;;
    3)
        echo "Running: Quantized Index Test (SQ8)"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 50 2000 \
            --limit 3000 \
            --quantization SQ8 \
            --similarity Euclidean \
            --train-list 3000
        ;;
    4)
        echo "Running: Insert/Delete/Verify Test (Standard)"
        echo ""
        echo "Step 1: Load initial dataset [0, 2999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 --no-query --limit 3000
        echo ""
        echo "Step 2: Insert records [3000, 5999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --insert 3000 5999
        echo ""
        echo "Step 3: Delete records [2000, 3999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --delete 2000 3999
        echo ""
        echo "Step 4: Verify query (expect intervals [0, 1999] and [4000, 5999])"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --verify 400 0 1999 4000 5999
        ;;
    5)
        echo "Running: Insert/Delete/Verify Test (Quantized SQ7)"
        echo ""
        echo "Step 1: Load initial dataset with quantization [0, 2999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET 153 --no-query \
            --limit 3000 \
            --quantization SQ7 \
            --similarity euclidean-squared \
            --train-list 3000
        echo ""
        echo "Step 2: Insert records [3000, 5999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --insert 3000 5999
        echo ""
        echo "Step 3: Delete records [2000, 3999]"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --delete 2000 3999
        echo ""
        echo "Step 4: Verify query (expect intervals [0, 1999] and [4000, 5999])"
        python "$SCRIPT_DIR/movie_index_test.py" $DATASET --verify 400 0 1999 4000 5999
        ;;
    *)
        echo "Invalid option. Please choose 1-5."
        exit 1
        ;;
esac

echo ""
echo "✓ Test completed successfully!"
