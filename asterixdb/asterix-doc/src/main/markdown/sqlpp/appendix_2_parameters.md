<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

The `SET` statement can be used to override some cluster-wide configuration parameters for a specific request:

##### SetStmnt
![](../images/diagrams/SetStmnt.png)

As parameter identifiers are qualified names (containing a '.') they have to be escaped using backticks (\`\`).
Note that changing query parameters will not affect query correctness but only impact performance
characteristics, such as response time and throughput.

## <a id="Parallelism_parameter">Parallelism Parameter</a>
The system can execute each request using multiple cores on multiple machines (a.k.a., partitioned parallelism)
in a cluster. A user can manually specify the maximum execution parallelism for a request to scale it up and down
using the following parameter:

*  **compiler.parallelism**: the maximum number of CPU cores can be used to process a query.
   There are three cases of the value *p* for compiler.parallelism:

     - *p* \< 0 or *p* \> the total number of cores in a cluster:  the system will use all available cores in the
       cluster;

     - *p* = 0 (the default):  the system will use the storage parallelism (the number of partitions of stored datasets)
       as the maximum parallelism for query processing;

     - all other cases:  the system will use the user-specified number as the maximum number of CPU cores to use for
       executing the query.

##### Example

    SET `compiler.parallelism` "16";

    SELECT c.name AS cname, o.orderno AS orderno
    FROM customers c JOIN orders o ON c.custid = o.custid;

## <a id="Memory_parameters">Memory Parameters</a>

In the system, each blocking runtime operator such as join, group-by and order-by
works within a fixed memory budget, and can gracefully spill to disks if
the memory budget is smaller than the amount of data they have to hold.
A user can manually configure the memory budget of those operators within a query.
The supported configurable memory parameters are:

*  **compiler.groupmemory**: the memory budget that each parallel group-by operator instance can use;
   32MB is the default budget.

*  **compiler.sortmemory**: the memory budget that each parallel sort operator instance can use;
   32MB is the default budget.

*  **compiler.joinmemory**: the memory budget that each parallel hash join operator instance can use;
   32MB is the default budget.

*  **compiler.windowmemory**: the memory budget that each parallel window aggregate operator instance can use;
   32MB is the default budget.

For each memory budget value, you can use a 64-bit integer value
with a 1024-based binary unit suffix (for example, B, KB, MB, GB).
If there is no user-provided suffix, "B" is the default suffix. See the following examples.

##### Example

    SET `compiler.groupmemory` "64MB";

    SELECT c.custid, COUNT(*)
    FROM customers c
    GROUP BY c.custid;

##### Example

    SET `compiler.sortmemory` "67108864";

    SELECT VALUE o
    FROM orders AS o
    ORDER BY ARRAY_LENGTH(o.items) DESC;

##### Example

    SET `compiler.joinmemory` "132000KB";

    SELECT c.name AS cname, o.ordeno AS orderno
    FROM customers c JOIN orders o ON c.custid = o.custid;

## <a id="Vector_topdown_parameters">Vector index top-down build parameters</a>

These session parameters tune the BKT-style top-down hierarchical clustering used during **VTREE index static-structure
build**. They apply only when `SET` appears **before** `CREATE INDEX` in the same request.

* **`compiler.vector.topdown.lambdaFactor`**: optional fixed balance factor for lambda-balanced k-means. When omitted,
  the build auto-tunes λ once per partition (SPANN `DynamicFactorSelect`). Set a positive value (e.g. `100`) to fix λ.

* **`compiler.vector.topdown.maxlevel`**: strict height cap as deepest level index (default `4`, i.e. levels `0..4`).

* **`compiler.vector.topdown.v`** and **`compiler.vector.topdown.gamma`**: deprecated; no effect on the current BKT-style
  top-down build (fan-out is dynamic, capped at 32, with lambda balancing instead of FSCL).

##### Example

    SET `compiler.vector.topdown.lambdaFactor` "100";
    SET `compiler.vector.topdown.maxlevel` "4";

    CREATE INDEX vecIdx ON myDataset(embedding) TYPE vctree
    WITH {"dimension": 384, "similarity": "cosine", "num_clusters": 157, "quantization": "SQ8", "structure_build": "spann"};

## <a id="Vector_selecthead_parameters">Vector index SelectHead / BuildHead parameters</a>

These session parameters control SPANN-style **SelectHead** + **BuildHead** during VTREE static-structure build.
SelectHead applies only when `structure_build` is `"spann"` in the index WITH clause; the routing tree is built from
head vectors when SelectHead is enabled (`num_clusters` is ignored for structure stop). Use `SET` **before**
`CREATE INDEX` in the same request.

* **`compiler.vector.selecthead.enabled`**: `true` (default) to run SelectHead + BuildHead; `false` for full-sample top-down.

* **`compiler.vector.selecthead.headRatio`**: target fraction of sample records to select as heads (default `0.12`).

* **`compiler.vector.selecthead.headCount`**: optional explicit head count; when set, overrides `headRatio`.

* **`compiler.vector.selecthead.selectType`**: `bkt` (default) or `random`.

* **`compiler.vector.selecthead.bktLeafSize`**: optional scratch BKT leaf stop threshold for SelectHead; when unset, page-derived leaf capacity is used.

* **Lambda**: BuildHead re-tunes `compiler.vector.topdown.lambdaFactor` on the head subset when that SET value is omitted.

* **Empty buckets**: k-means splits may produce buckets with zero assigned records; these are omitted from the routing tree (no SET parameter).

##### Example

    SET `compiler.vector.selecthead.headRatio` "0.2";

    CREATE INDEX vecIdx ON myDataset(embedding) TYPE vctree
    WITH {"dimension": 384, "similarity": "euclidean", "num_clusters": 1000, "quantization": "SQ8", "structure_build": "spann"};

