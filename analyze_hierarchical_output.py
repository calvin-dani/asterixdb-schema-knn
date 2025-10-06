#!/usr/bin/env python3
"""
Detailed analysis of the hierarchical k-means JSON output
"""

import json
import numpy as np
from collections import defaultdict

def analyze_hierarchical_json(json_file):
    """Analyze the hierarchical structure in detail"""
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    analysis = {
        "file_info": {
            "file_path": json_file,
            "file_size": len(json.dumps(data, indent=2))
        },
        "structure": {},
        "clusters": {},
        "hierarchy": {},
        "quality_metrics": {}
    }
    
    # Analyze overall structure
    analysis["structure"] = {
        "total_levels": data.get("total_levels", 0),
        "levels": len(data.get("levels", [])),
        "keys": list(data.keys())
    }
    
    # Analyze each level
    levels = data.get("levels", [])
    analysis["hierarchy"]["levels"] = {}
    
    for i, level in enumerate(levels):
        level_info = {
            "level_id": i,
            "centroids": level.get("centroids", []),
            "centroid_count": level.get("centroid_count", 0),
            "level": level.get("level", i)
        }
        
        # Analyze centroids in this level
        centroids = level.get("centroids", [])
        level_info["centroid_analysis"] = {
            "count": len(centroids),
            "dimensions": [],
            "has_parents": [],
            "parent_ids": [],
            "global_ids": []
        }
        
        for centroid in centroids:
            coords = centroid.get("coordinates", [])
            level_info["centroid_analysis"]["dimensions"].append(len(coords))
            level_info["centroid_analysis"]["has_parents"].append(centroid.get("has_parent", False))
            level_info["centroid_analysis"]["parent_ids"].append(centroid.get("parent_cluster_id"))
            level_info["centroid_analysis"]["global_ids"].append(centroid.get("global_id"))
        
        analysis["hierarchy"]["levels"][f"level_{i}"] = level_info
    
    # Analyze cluster relationships
    parent_child_map = defaultdict(list)
    child_parent_map = {}
    
    for level in levels:
        for centroid in level.get("centroids", []):
            if centroid.get("has_parent", False):
                parent_id = centroid.get("parent_cluster_id")
                child_id = centroid.get("global_id")
                parent_child_map[parent_id].append(child_id)
                child_parent_map[child_id] = parent_id
    
    analysis["hierarchy"]["relationships"] = {
        "parent_child_map": dict(parent_child_map),
        "child_parent_map": child_parent_map,
        "orphan_clusters": [cid for cid in child_parent_map.values() if cid not in parent_child_map]
    }
    
    # Calculate quality metrics
    all_coords = []
    for level in levels:
        for centroid in level.get("centroids", []):
            coords = centroid.get("coordinates", [])
            if coords:
                all_coords.append(coords)
    
    if all_coords:
        coords_array = np.array(all_coords)
        analysis["quality_metrics"] = {
            "total_centroids": len(all_coords),
            "dimension": len(all_coords[0]) if all_coords else 0,
            "coordinate_ranges": {
                "min": np.min(coords_array).item(),
                "max": np.max(coords_array).item(),
                "mean": np.mean(coords_array).item(),
                "std": np.std(coords_array).item()
            }
        }
    
    return analysis

def print_analysis(analysis):
    """Print a formatted analysis"""
    
    print("🔍 Detailed Hierarchical K-Means Analysis")
    print("=" * 60)
    
    # File info
    print(f"\n📁 File Information:")
    print(f"   Path: {analysis['file_info']['file_path']}")
    print(f"   Size: {analysis['file_info']['file_size']:,} characters")
    
    # Structure
    print(f"\n🏗️  Structure:")
    print(f"   Total levels: {analysis['structure']['total_levels']}")
    print(f"   Levels found: {analysis['structure']['levels']}")
    print(f"   Keys: {', '.join(analysis['structure']['keys'])}")
    
    # Hierarchy analysis
    print(f"\n🌳 Hierarchy Analysis:")
    for level_name, level_info in analysis['hierarchy']['levels'].items():
        print(f"\n   {level_name.upper()}:")
        print(f"     Level ID: {level_info['level_id']}")
        print(f"     Centroid count: {level_info['centroid_count']}")
        print(f"     Actual centroids: {len(level_info['centroids'])}")
        
        centroid_analysis = level_info['centroid_analysis']
        print(f"     Dimensions: {set(centroid_analysis['dimensions'])}")
        print(f"     Has parents: {sum(centroid_analysis['has_parents'])}/{len(centroid_analysis['has_parents'])}")
        print(f"     Global IDs: {centroid_analysis['global_ids']}")
        if any(centroid_analysis['parent_ids']):
            print(f"     Parent IDs: {[pid for pid in centroid_analysis['parent_ids'] if pid is not None]}")
    
    # Relationships
    print(f"\n🔗 Cluster Relationships:")
    relationships = analysis['hierarchy']['relationships']
    print(f"   Parent-child mappings: {len(relationships['parent_child_map'])} parents")
    for parent, children in relationships['parent_child_map'].items():
        print(f"     Parent {parent} -> Children {children}")
    
    if relationships['orphan_clusters']:
        print(f"   Orphan clusters: {relationships['orphan_clusters']}")
    
    # Quality metrics
    if analysis['quality_metrics']:
        print(f"\n📊 Quality Metrics:")
        qm = analysis['quality_metrics']
        print(f"   Total centroids: {qm['total_centroids']}")
        print(f"   Dimension: {qm['dimension']}")
        print(f"   Coordinate ranges:")
        print(f"     Min: {qm['coordinate_ranges']['min']:.6f}")
        print(f"     Max: {qm['coordinate_ranges']['max']:.6f}")
        print(f"     Mean: {qm['coordinate_ranges']['mean']:.6f}")
        print(f"     Std: {qm['coordinate_ranges']['std']:.6f}")
    
    # Algorithm assessment
    print(f"\n🎯 Algorithm Assessment:")
    
    # Check if hierarchy is properly formed
    levels = analysis['structure']['levels']
    if levels >= 2:
        print("   ✅ Multi-level hierarchy detected")
    else:
        print("   ⚠️  Single level hierarchy (expected for small datasets)")
    
    # Check centroid distribution
    total_centroids = analysis['quality_metrics'].get('total_centroids', 0)
    if total_centroids > 0:
        print(f"   ✅ {total_centroids} centroids generated")
    else:
        print("   ❌ No centroids generated")
    
    # Check parent-child relationships
    parent_count = len(relationships['parent_child_map'])
    if parent_count > 0:
        print(f"   ✅ {parent_count} parent-child relationships found")
    else:
        print("   ⚠️  No parent-child relationships (flat hierarchy)")
    
    # Check coordinate quality
    if analysis['quality_metrics']:
        coord_std = analysis['quality_metrics']['coordinate_ranges']['std']
        if coord_std > 0:
            print(f"   ✅ Coordinate variance detected (std: {coord_std:.3f})")
        else:
            print("   ⚠️  No coordinate variance (possible issue)")

def main():
    """Main analysis function"""
    
    json_file = "/home/calvin-dani/IdeaProjects/asterixdb-schema-knn/workspace_1759732199222_0/cluster_indexes/hierarchical_cluster_index.json"
    
    try:
        analysis = analyze_hierarchical_json(json_file)
        print_analysis(analysis)
        
        # Save detailed analysis
        with open("detailed_hierarchical_analysis.json", "w") as f:
            json.dump(analysis, f, indent=2)
        print(f"\n💾 Detailed analysis saved to: detailed_hierarchical_analysis.json")
        
    except Exception as e:
        print(f"❌ Error analyzing JSON file: {e}")

if __name__ == "__main__":
    main()
