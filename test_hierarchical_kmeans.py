#!/usr/bin/env python3
"""
Test script to evaluate the HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor
This script creates synthetic test data and analyzes the algorithm's output.
"""

import json
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score
import os
import glob
from datetime import datetime

def create_synthetic_test_data():
    """Create synthetic test data with known cluster structure"""
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Create 3 well-separated clusters
    cluster1 = np.random.normal([1.0, 1.0, 1.0], 0.1, (10, 3))  # Cluster around (1,1,1)
    cluster2 = np.random.normal([5.0, 5.0, 5.0], 0.1, (10, 3))  # Cluster around (5,5,5)
    cluster3 = np.random.normal([10.0, 10.0, 10.0], 0.1, (10, 3))  # Cluster around (10,10,10)
    
    # Add some edge cases
    edge_cases = np.array([
        [1.0, 1.0, 1.0],  # Duplicate point
        [1.0, 1.0, 1.0],  # Another duplicate
        [50.0, 50.0, 50.0],  # Outlier
        [100.0, 100.0, 100.0]  # Another outlier
    ])
    
    # Combine all data
    data = np.vstack([cluster1, cluster2, cluster3, edge_cases])
    
    # Create test dataset in AsterixDB format
    test_data = []
    for i, point in enumerate(data):
        test_data.append({
            "id": i + 1,
            "embedding": point.tolist()
        })
    
    return test_data, np.vstack([cluster1, cluster2, cluster3])

def analyze_clustering_quality(data, true_labels=None):
    """Analyze the quality of clustering results"""
    
    if len(data) == 0:
        return {"error": "No data to analyze"}
    
    # Extract embeddings
    embeddings = np.array([item["embedding"] for item in data])
    
    # Extract cluster assignments if available
    cluster_assignments = []
    if "cluster_id" in data[0]:
        cluster_assignments = [item.get("cluster_id", -1) for item in data]
    else:
        # If no cluster assignments, try to infer from the data structure
        cluster_assignments = [item.get("cluster_info", {}).get("cluster_id", -1) for item in data]
    
    # Filter out unassigned points
    valid_assignments = [i for i, c in enumerate(cluster_assignments) if c != -1]
    
    if len(valid_assignments) == 0:
        return {"error": "No valid cluster assignments found"}
    
    valid_embeddings = embeddings[valid_assignments]
    valid_clusters = [cluster_assignments[i] for i in valid_assignments]
    
    # Calculate clustering metrics
    metrics = {
        "total_points": len(data),
        "clustered_points": len(valid_assignments),
        "num_clusters": len(set(valid_clusters)),
        "cluster_distribution": {}
    }
    
    # Cluster size distribution
    for cluster_id in set(valid_clusters):
        count = valid_clusters.count(cluster_id)
        metrics["cluster_distribution"][f"cluster_{cluster_id}"] = count
    
    # Calculate intra-cluster distances
    intra_cluster_distances = []
    for cluster_id in set(valid_clusters):
        cluster_points = valid_embeddings[np.array(valid_clusters) == cluster_id]
        if len(cluster_points) > 1:
            # Calculate average distance within cluster
            distances = []
            for i in range(len(cluster_points)):
                for j in range(i + 1, len(cluster_points)):
                    dist = np.linalg.norm(cluster_points[i] - cluster_points[j])
                    distances.append(dist)
            if distances:
                intra_cluster_distances.extend(distances)
    
    if intra_cluster_distances:
        metrics["avg_intra_cluster_distance"] = np.mean(intra_cluster_distances)
        metrics["std_intra_cluster_distance"] = np.std(intra_cluster_distances)
    
    # Calculate silhouette score if we have enough clusters
    if len(set(valid_clusters)) > 1 and len(valid_embeddings) > len(set(valid_clusters)):
        try:
            silhouette = silhouette_score(valid_embeddings, valid_clusters)
            metrics["silhouette_score"] = silhouette
        except:
            metrics["silhouette_score"] = "Could not calculate"
    
    # Calculate Calinski-Harabasz score
    if len(set(valid_clusters)) > 1:
        try:
            ch_score = calinski_harabasz_score(valid_embeddings, valid_clusters)
            metrics["calinski_harabasz_score"] = ch_score
        except:
            metrics["calinski_harabasz_score"] = "Could not calculate"
    
    return metrics

def find_latest_json_output():
    """Find the latest JSON output file from the hierarchical k-means algorithm"""
    
    # Look for JSON files in common output directories
    search_patterns = [
        "/home/calvin-dani/IdeaProjects/asterixdb-schema-knn/workspace_*/cluster_indexes/*.json",
        "/home/calvin-dani/IdeaProjects/asterixdb-schema-knn/target/*/cluster_indexes/*.json",
        "/home/calvin-dani/IdeaProjects/asterixdb-schema-knn/**/*cluster*.json"
    ]
    
    latest_file = None
    latest_time = 0
    
    for pattern in search_patterns:
        files = glob.glob(pattern, recursive=True)
        for file in files:
            if os.path.isfile(file):
                mtime = os.path.getmtime(file)
                if mtime > latest_time:
                    latest_time = mtime
                    latest_file = file
    
    return latest_file

def analyze_hierarchical_structure(json_file):
    """Analyze the hierarchical structure from JSON output"""
    
    if not json_file or not os.path.exists(json_file):
        return {"error": f"JSON file not found: {json_file}"}
    
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        analysis = {
            "file_path": json_file,
            "file_size": os.path.getsize(json_file),
            "last_modified": datetime.fromtimestamp(os.path.getmtime(json_file)).isoformat()
        }
        
        # Analyze the structure
        if isinstance(data, dict):
            analysis["structure_type"] = "dictionary"
            analysis["keys"] = list(data.keys())
            
            # Look for hierarchical information
            if "levels" in data:
                analysis["num_levels"] = len(data["levels"])
                analysis["level_info"] = {}
                for i, level in enumerate(data["levels"]):
                    analysis["level_info"][f"level_{i}"] = {
                        "num_clusters": len(level) if isinstance(level, list) else 1,
                        "type": type(level).__name__
                    }
            
            if "clusters" in data:
                analysis["num_clusters"] = len(data["clusters"])
            
            if "tree" in data:
                analysis["has_tree_structure"] = True
                analysis["tree_type"] = type(data["tree"]).__name__
        
        elif isinstance(data, list):
            analysis["structure_type"] = "list"
            analysis["num_items"] = len(data)
            
            # Analyze list items
            if len(data) > 0:
                analysis["item_types"] = [type(item).__name__ for item in data[:5]]  # First 5 items
                if isinstance(data[0], dict):
                    analysis["common_keys"] = list(data[0].keys())
        
        else:
            analysis["structure_type"] = type(data).__name__
            analysis["value"] = str(data)[:100]  # First 100 chars
        
        return analysis
        
    except Exception as e:
        return {"error": f"Error analyzing JSON file: {str(e)}"}

def main():
    """Main test function"""
    
    print("🔬 Hierarchical K-Means Algorithm Evaluation")
    print("=" * 50)
    
    # 1. Create synthetic test data
    print("\n1. Creating synthetic test data...")
    test_data, true_clusters = create_synthetic_test_data()
    print(f"   ✓ Created {len(test_data)} test points")
    print(f"   ✓ Data shape: {true_clusters.shape}")
    
    # 2. Analyze clustering quality on synthetic data
    print("\n2. Analyzing clustering quality...")
    quality_metrics = analyze_clustering_quality(test_data)
    
    if "error" in quality_metrics:
        print(f"   ❌ Error: {quality_metrics['error']}")
    else:
        print(f"   ✓ Total points: {quality_metrics['total_points']}")
        print(f"   ✓ Clustered points: {quality_metrics['clustered_points']}")
        print(f"   ✓ Number of clusters: {quality_metrics['num_clusters']}")
        if 'silhouette_score' in quality_metrics:
            print(f"   ✓ Silhouette score: {quality_metrics['silhouette_score']:.3f}")
        if 'calinski_harabasz_score' in quality_metrics:
            print(f"   ✓ Calinski-Harabasz score: {quality_metrics['calinski_harabasz_score']:.3f}")
    
    # 3. Look for actual algorithm output
    print("\n3. Looking for algorithm output files...")
    latest_json = find_latest_json_output()
    
    if latest_json:
        print(f"   ✓ Found output file: {latest_json}")
        
        # Analyze the hierarchical structure
        print("\n4. Analyzing hierarchical structure...")
        structure_analysis = analyze_hierarchical_structure(latest_json)
        
        if "error" in structure_analysis:
            print(f"   ❌ Error: {structure_analysis['error']}")
        else:
            print(f"   ✓ File size: {structure_analysis['file_size']} bytes")
            print(f"   ✓ Last modified: {structure_analysis['last_modified']}")
            print(f"   ✓ Structure type: {structure_analysis['structure_type']}")
            
            if "num_levels" in structure_analysis:
                print(f"   ✓ Number of levels: {structure_analysis['num_levels']}")
            
            if "num_clusters" in structure_analysis:
                print(f"   ✓ Number of clusters: {structure_analysis['num_clusters']}")
            
            if "level_info" in structure_analysis:
                print("   ✓ Level information:")
                for level, info in structure_analysis["level_info"].items():
                    print(f"     - {level}: {info['num_clusters']} clusters")
    
    else:
        print("   ⚠️  No JSON output files found")
        print("   💡 This suggests the algorithm may not be producing output files")
        print("   💡 Check if the HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor is complete")
    
    # 4. Generate evaluation report
    print("\n5. Generating evaluation report...")
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "test_data": {
            "num_points": len(test_data),
            "data_shape": true_clusters.shape,
            "clusters_created": 3
        },
        "quality_metrics": quality_metrics,
        "algorithm_output": {
            "json_file_found": latest_json is not None,
            "json_file_path": latest_json,
            "structure_analysis": structure_analysis if latest_json else None
        }
    }
    
    # Save report
    report_file = f"hierarchical_kmeans_evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"   ✓ Report saved to: {report_file}")
    
    # 5. Recommendations
    print("\n6. Recommendations:")
    
    if not latest_json:
        print("   🔧 Complete the SecondaryVectorOperationsHelper implementation")
        print("   🔧 Add bulk load operation to write data to index")
        print("   🔧 Ensure the algorithm produces JSON output files")
    
    if "error" in quality_metrics:
        print("   🔧 Fix clustering algorithm to produce valid assignments")
    
    if latest_json and "error" not in structure_analysis:
        print("   ✅ Algorithm appears to be working and producing output")
        print("   📊 Analyze the JSON structure for hierarchical correctness")
    
    print("\n🎯 Evaluation complete!")

if __name__ == "__main__":
    main()
