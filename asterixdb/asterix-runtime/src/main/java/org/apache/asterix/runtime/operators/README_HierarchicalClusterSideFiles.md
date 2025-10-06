<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 -->

# Hierarchical Cluster Side Files

This document explains how to use the side file functionality for storing hierarchical cluster levels in JSON format with a static structure index name.

## Overview

The hierarchical K-means operator now creates side files that contain:
- **Static Structure Index Name**: `hierarchical_cluster_index.json`
- **Complete cluster hierarchy** with parent-child relationships
- **Metadata** about the clustering process
- **Statistics** about the cluster structure

## Files Created

### 1. Side File (Managed by Hyracks)
- **Location**: Managed workspace file (automatically cleaned up)
- **Purpose**: Temporary storage during job execution
- **Format**: JSON

### 2. Static Index File (Manually Managed)
- **Location**: `{nodeId}/cluster_indexes/hierarchical_cluster_index.json`
- **Purpose**: Persistent storage for manual access
- **Format**: JSON
- **Name**: `hierarchical_cluster_index.json` (static, configurable)

## JSON Structure

```json
{
  "metadata": {
    "index_name": "hierarchical_cluster_index.json",
    "job_id": "job_12345",
    "partition": 0,
    "created_timestamp": 1234567890,
    "version": "1.0",
    "description": "Hierarchical K-means cluster index"
  },
  "cluster_levels": [
    {
      "level": 0,
      "centroid_count": 100,
      "centroids": [
        {
          "cluster_id": 0,
          "global_id": 1,
          "level": 0,
          "has_parent": false,
          "coordinates": [1.0, 2.0, 3.0],
          "dimension": 3
        }
      ]
    }
  ],
  "hierarchy_structure": {
    "total_levels": 3,
    "total_centroids": 150,
    "parent_child_relationships": [
      {
        "child_global_id": 2,
        "child_cluster_id": 0,
        "child_level": 1,
        "parent_cluster_id": 0,
        "parent_level": 0
      }
    ]
  },
  "statistics": {
    "total_centroids": 150,
    "total_levels": 3,
    "max_centroids_per_level": 100,
    "min_centroids_per_level": 5,
    "average_centroids_per_level": 50.0
  }
}
```

## Usage Examples

### 1. Reading the Side File

```java
import org.apache.asterix.runtime.operators.HierarchicalClusterIndexReader;

// Create reader
HierarchicalClusterIndexReader reader = new HierarchicalClusterIndexReader();

// Load the index
reader.loadIndex("cluster_indexes/hierarchical_cluster_index.json");

// Print summary
reader.printIndexSummary();

// Get specific level
Map<String, Object> level0 = reader.getClusterLevel(0);
System.out.println("Level 0 has " + level0.get("centroid_count") + " centroids");

// Get centroids for a level
List<Map<String, Object>> centroids = reader.getCentroidsForLevel(0);
for (Map<String, Object> centroid : centroids) {
    System.out.println("Cluster " + centroid.get("cluster_id") + 
                     " at coordinates " + centroid.get("coordinates"));
}
```

### 2. Traversing the Hierarchy

```java
// Get root centroids
List<Map<String, Object>> rootCentroids = reader.getRootCentroids();

// Find children of a specific centroid
long parentGlobalId = 1L;
List<Map<String, Object>> children = reader.getChildrenOfCentroid(parentGlobalId);

// Find parent of a specific centroid
long childGlobalId = 2L;
Map<String, Object> parent = reader.getParentOfCentroid(childGlobalId);
```

### 3. Finding Centroids by Criteria

```java
// Find centroid by global ID
Map<String, Object> centroid = reader.findCentroidByGlobalId(123L);

// Get all centroids at leaf level (highest level)
List<Map<String, Object>> leafCentroids = reader.getLeafCentroids();

// Get statistics
Map<String, Object> stats = reader.getStatistics();
System.out.println("Total centroids: " + stats.get("total_centroids"));
```

### 4. Exporting and Validation

```java
// Validate the index structure
if (reader.validateIndex()) {
    System.out.println("Index is valid!");
}

// Export to different location
reader.exportIndex("exported_index.json");

// Print detailed level information
reader.printLevelDetails(0);
```

## Integration with Hierarchical K-means Operator

The side file creation is automatically integrated into the `HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor`:

```java
// In the operator's initialize() method
HierarchicalClusterIndexWriter indexWriter = new HierarchicalClusterIndexWriter(ctx, partition);

// Build hierarchical clustering (automatically populates index)
buildHierarchicalClustering(ctx, in, hierarchicalState, fta, tuple, eval, inputVal, 
                           listAccessorConstant, KMeansUtils, vSizeFrame, appender, partition, indexWriter);

// Write the index to side file
indexWriter.writeIndexToSideFile();

// Also write to static location for manual management
indexWriter.writeIndex();
```

## Configuration

### Static Structure Index Name
The index file name is defined as a constant:
```java
private static final String STATIC_STRUCTURE_INDEX_NAME = "hierarchical_cluster_index.json";
```

### Index Directory
The directory where the static index is stored:
```java
private static final String INDEX_DIRECTORY = "cluster_indexes";
```

## File Locations

1. **Side File**: Managed by Hyracks framework (temporary)
   - Path: `{workspace}/hierarchical_cluster_index_{partition}`
   - Lifecycle: Created during job, cleaned up after completion

2. **Static Index File**: Manually managed (persistent)
   - Path: `{nodeId}/cluster_indexes/hierarchical_cluster_index.json`
   - Lifecycle: Created during job, persists for manual access

## Error Handling

The system includes comprehensive error handling:

```java
try {
    reader.loadIndex(indexPath);
    if (reader.validateIndex()) {
        // Process index
    }
} catch (IOException e) {
    System.err.println("Error loading index: " + e.getMessage());
}
```

## Performance Considerations

- **Memory Efficient**: Index is written incrementally as levels are built
- **Streaming**: Large hierarchies don't require loading all data into memory
- **Validation**: Built-in validation ensures index integrity
- **Statistics**: Automatic generation of cluster statistics

## Dependencies

The side file functionality requires:
- Jackson JSON library for JSON processing
- AsterixDB runtime framework
- Hyracks dataflow framework

## Example Output

When the hierarchical K-means operator runs, you'll see output like:

```
Hierarchical cluster index written to side file: /workspace/hierarchical_cluster_index_0
Hierarchical cluster index written to: node1/cluster_indexes/hierarchical_cluster_index.json

Hierarchical Cluster Index Summary:
=====================================
Index Name: hierarchical_cluster_index.json
Job ID: job_12345
Partition: 0
Total Levels: 3
  Level 0: 100 centroids
  Level 1: 25 centroids
  Level 2: 5 centroids
Total Centroids: 130
Index Path: node1/cluster_indexes/hierarchical_cluster_index.json
==========================================
```

This provides a complete solution for creating, managing, and accessing hierarchical cluster side files in JSON format with a static structure index name! 🚀
