"""AsterixDB HTTP client and SQL++ helpers."""

import json
import os
import re
import time
import requests


class AsterixDBClient:
    """HTTP client for executing SQL++ statements against AsterixDB."""

    def __init__(self, url, timeout=600):
        self.url = url
        self.timeout = timeout
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}

    def execute(self, statement, context_id="default", extra_params=None):
        """Execute a SQL++ statement and return parsed JSON response."""
        data = {
            "statement": statement,
            "pretty": "true",
            "client_context_id": context_id,
        }
        if extra_params:
            data.update(extra_params)

        resp = requests.post(
            self.url, headers=self.headers, data=data, timeout=self.timeout
        )
        if resp.status_code >= 400:
            print(f"  [ERROR] HTTP {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
        result = resp.json()
        status = result.get("status", "")
        if status not in ("success", "running"):
            errors = result.get("errors", [])
            raise RuntimeError(f"Statement failed ({status}): {errors}")
        return result

    def wait_for_ready(self, health_url, timeout=120):
        """Poll health endpoint until cluster is ACTIVE."""
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                resp = requests.get(health_url, timeout=5)
                if resp.status_code == 200:
                    body = resp.json()
                    state = body.get("state", "")
                    if state == "ACTIVE":
                        print("[lifecycle] Cluster is ACTIVE")
                        return True
                    print(f"[lifecycle] Cluster state: {state}, waiting...")
            except Exception as e:
                last_err = str(e)
            time.sleep(2)
        raise TimeoutError(
            f"Cluster not ready after {timeout}s. Last error: {last_err}"
        )

    def create_dataverse(self, dataverse):
        """Drop and recreate a dataverse."""
        stmt = f"""
        DROP DATAVERSE {dataverse} IF EXISTS;
        CREATE DATAVERSE {dataverse};
        """
        self.execute(stmt, f"create_dv_{dataverse}")

    def create_type_and_dataset(self, dataverse, type_name, dataset_name,
                                pk_field, type_schema):
        """Create a type and dataset with column storage format."""
        stmt = f"""
        USE {dataverse};
        CREATE TYPE {type_name} AS {{ {type_schema} }};
        CREATE DATASET {dataset_name}({type_name}) PRIMARY KEY {pk_field}
          WITH {{"storage-format":{{"format":"column"}}}};
        """
        self.execute(stmt, f"create_ds_{dataset_name}")

    def load_dataset(self, dataverse, dataset_name, file_path):
        """Load data from local filesystem into dataset."""
        abs_path = os.path.abspath(file_path).replace("\\", "/")
        localfs_path = f"localhost://{abs_path}"
        stmt = f"""
        USE {dataverse};
        LOAD DATASET {dataset_name} USING localfs (
          ("path" = "{localfs_path}"),
          ("format" = "json")
        );
        """
        self.execute(stmt, f"load_{dataset_name}")

    def analyze_dataset(self, dataverse, dataset_name):
        """Run ANALYZE DATASET (required before vector index creation)."""
        stmt = f"""
        USE {dataverse};
        ANALYZE DATASET {dataset_name};
        """
        self.execute(stmt, f"analyze_{dataset_name}")

    def create_vector_index(self, dataverse, dataset_name, index_name,
                            vector_field, dimension,
                            similarity, train_list_fraction=0.1,
                            epsilon=0.3, include_fields=None,
                            cross_pollination_m=1, rng_factor=1.0):
        """Create a vector index with optional INCLUDE fields.

        Quantization is always-on at the backend (defaults to SQ8) and is no
        longer a user-facing knob in the DDL WITH-clause.

        cross_pollination_m > 1 replicates each record into the M closest leaf
        centroids at bulk-load AND at incremental insert/delete (rng_factor
        thins the candidate set, SPTAG-style; rng_factor=1 is strict and rarely
        admits >1 replica, so use ~3 to actually exercise multi-cluster
        placement). M=1 omits both knobs entirely (legacy single-closest DDL).
        """
        include_clause = ""
        if include_fields:
            include_clause = f"INCLUDE({', '.join(include_fields)})"

        with_params = {
            "dimension": dimension,
            "similarity": f'"{similarity}"',
            "train_list_fraction": train_list_fraction,
            "epsilon": epsilon,
        }
        if cross_pollination_m and cross_pollination_m > 1:
            with_params["cross_pollination_m"] = cross_pollination_m
            with_params["rng_factor"] = rng_factor

        with_entries = [f'"{k}": {v}' for k, v in with_params.items()]
        with_clause = ", ".join(with_entries)

        trainseed = os.environ.get("VTREE_TRAINSEED", "42")
        stmt = f"""
        USE {dataverse};
        SET `compiler.vector.trainseed` "{trainseed}";
        CREATE INDEX {index_name}
          ON {dataset_name}({vector_field} VECTOR) {include_clause}
          TYPE VTREE
          WITH {{ {with_clause} }} EXCLUDE UNKNOWN KEY;
        """
        self.execute(stmt, f"create_ix_{index_name}")

    def count_records(self, dataverse, dataset_name):
        """Return record count for a dataset."""
        stmt = f"USE {dataverse}; SELECT VALUE COUNT(*) FROM {dataset_name};"
        result = self.execute(stmt, f"count_{dataset_name}")
        return result["results"][0]

    def fetch_embedding(self, dataverse, dataset_name, pk_field, pk_value,
                        vector_field):
        """Fetch a single embedding vector by primary key."""
        stmt = f"""
        USE {dataverse};
        SELECT m.{vector_field}
        FROM {dataset_name} m
        WHERE m.{pk_field} = {pk_value};
        """
        result = self.execute(stmt, f"fetch_emb_{pk_value}")
        if result.get("results"):
            return result["results"][0].get(vector_field)
        return None

    def run_ann_query(self, dataverse, dataset_name, vector_field, qvec,
                      similarity, k, min_probe_fraction, k_multiplier,
                      where_clause=None):
        """Execute an ANN query and return results dict with metrics."""
        similarity = _map_similarity(similarity)
        qvec_str = json.dumps(qvec)
        where_line = f"WHERE {where_clause}" if where_clause else ""
        pk_field = "idx"  # standard across our datasets

        stmt = f"""
        USE {dataverse};
        LET qvec = {qvec_str}
        FROM {dataset_name} m
        LET dist = ann_distance(m.{vector_field}, qvec, "{similarity}", {min_probe_fraction}, {k_multiplier})
        {where_line}
        SELECT m.{pk_field}, dist
        ORDER BY dist
        LIMIT {k};
        """
        result = self.execute(stmt, "ann_query")
        metrics = result.get("metrics", {})
        return {
            "results": result.get("results", []),
            "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
            "execution_ms": parse_time_ms(metrics.get("executionTime", "0ms")),
        }

    def run_exact_knn_query(self, dataverse, dataset_name, vector_field, qvec,
                            similarity, k, where_clause=None):
        """Execute exact brute-force KNN query for recall comparison.

        Uses the public vector_distance(a, b, metric) sugar (no vector index path).
        """
        dist_expr = _distance_expr(similarity, vector_field)
        qvec_str = json.dumps(qvec)
        where_line = f"WHERE {where_clause}" if where_clause else ""
        pk_field = "idx"

        stmt = f"""
        USE {dataverse};
        LET qvec = {qvec_str}
        FROM {dataset_name} m
        LET dist = {dist_expr}
        {where_line}
        SELECT m.{pk_field}, dist
        ORDER BY dist
        LIMIT {k};
        """
        result = self.execute(stmt, "knn_query")
        metrics = result.get("metrics", {})
        return {
            "results": result.get("results", []),
            "elapsed_ms": parse_time_ms(metrics.get("elapsedTime", "0ms")),
            "execution_ms": parse_time_ms(metrics.get("executionTime", "0ms")),
        }

    def insert_from_staging(self, dataverse, staging, target, pk_field,
                            start, end):
        """INSERT INTO target (SELECT FROM staging WHERE pk in range)."""
        stmt = f"""
        USE {dataverse};
        INSERT INTO {target} (
          SELECT VALUE s
          FROM {staging} s
          WHERE s.{pk_field} >= {start} AND s.{pk_field} <= {end}
        );
        """
        self.execute(stmt, f"insert_{start}_{end}")

    def delete_by_pk_range(self, dataverse, dataset_name, pk_field, start,
                           end):
        """Delete records by primary key range."""
        stmt = f"""
        USE {dataverse};
        DELETE FROM {dataset_name} m
        WHERE m.{pk_field} >= {start} AND m.{pk_field} <= {end};
        """
        self.execute(stmt, f"delete_{start}_{end}")

    def fetch_all_pks(self, dataverse, dataset_name, pk_field, limit=100000):
        """Fetch all primary keys as a set."""
        stmt = f"""
        USE {dataverse};
        SELECT VALUE m.{pk_field}
        FROM {dataset_name} m
        ORDER BY m.{pk_field}
        LIMIT {limit};
        """
        result = self.execute(stmt, f"fetch_pks_{dataset_name}")
        return set(result.get("results", []))

    def run_ann_query_pks(self, dataverse, dataset_name, vector_field, qvec,
                          similarity, min_probe_fraction, k_multiplier,
                          limit, pk_field="idx"):
        """Run ANN query and return set of result PKs."""
        similarity = _map_similarity(similarity)
        qvec_str = json.dumps(qvec)
        stmt = f"""
        USE {dataverse};
        LET qvec = {qvec_str}
        FROM {dataset_name} m
        LET dist = ann_distance(m.{vector_field}, qvec, "{similarity}", {min_probe_fraction}, {k_multiplier})
        SELECT VALUE m.{pk_field}
        ORDER BY dist
        LIMIT {limit};
        """
        result = self.execute(stmt, "ann_query_pks")
        raw_results = result.get("results", [])
        # Diagnostic: always print raw count, check for duplicates and nulls
        null_count = sum(1 for r in raw_results if r is None)
        unique_count = len(set(r for r in raw_results if r is not None))
        print(f"  [DIAG] ANN query raw_results: count={len(raw_results)}, "
              f"unique={unique_count}, nulls={null_count}")
        if len(raw_results) != unique_count:
            print(f"  [DIAG]   dupes={len(raw_results) - unique_count - null_count}")
        return set(r for r in raw_results if r is not None)

    def run_bruteforce_query_pks(self, dataverse, dataset_name, vector_field,
                                 qvec, similarity, limit, pk_field="idx"):
        """Run brute-force per-metric distance query and return set of result PKs.
        This does NOT use the vector index — full table scan for comparison."""
        dist_expr = _distance_expr(similarity, vector_field)
        qvec_str = json.dumps(qvec)
        stmt = f"""
        USE {dataverse};
        LET qvec = {qvec_str}
        FROM {dataset_name} m
        LET dist = {dist_expr}
        SELECT VALUE m.{pk_field}
        ORDER BY dist
        LIMIT {limit};
        """
        result = self.execute(stmt, "bruteforce_query_pks")
        raw_results = result.get("results", [])
        print(f"  [DIAG] Brute-force query raw_results: count={len(raw_results)}")
        return set(r for r in raw_results if r is not None)

    def compact_dataset(self, dataverse, dataset_name):
        """Compact a dataset."""
        stmt = f"""
        USE {dataverse};
        COMPACT DATASET {dataset_name};
        """
        self.execute(stmt, f"compact_{dataset_name}")


# ---------------------------------------------------------------------------
# Standalone helpers
# ---------------------------------------------------------------------------

# Map index-level similarity names to the metric label passed to ann_distance(...)
_SIMILARITY_MAP = {
    "cosine": "cosine similarity",
    "euclidean": "euclidean",
    "euclidean_squared": "euclidean_squared",
    "dot": "dot",
    "dot_product": "dot_product",
}


def _map_similarity(similarity):
    """Map index similarity string to SQL++ ann_distance metric label."""
    return _SIMILARITY_MAP.get(similarity, similarity)


# Map index-level similarity names to the per-metric distance function name
# used for exact/brute-force KNN baselines (no vector index path).
_DISTANCE_FUNC_MAP = {
    "cosine": "cosine_distance",
    "euclidean": "euclidean_distance",
    "euclidean_squared": "euclidean_squared_distance",
    "dot": "dot_distance",
    "dot_product": "dot_distance",
}

# Map index-level similarity names to the metric literal understood by the public
# 3-arg vector_distance(a, b, metric) builtin. Used for the exact/brute-force KNN
# baseline. NOTE: the per-metric 2-arg builtins cosine_distance/dot_distance are
# registered PRIVATE in BuiltinFunctions (ASTERIXDB-3676), so calling them from a
# user query fails to resolve (ASX1081). vector_distance is the public sugar that
# rewrites to those private builtins at compile time, so use it uniformly.
_VECTOR_DISTANCE_METRIC_MAP = {
    "cosine": "cosine",
    "euclidean": "euclidean",
    "euclidean_squared": "euclidean_squared",
    "dot": "dot",
    "dot_product": "dot",
}


def _distance_func(similarity):
    """Resolve a per-metric SQL++ distance function (2-arg) for a similarity."""
    func = _DISTANCE_FUNC_MAP.get(similarity)
    if func is None:
        raise ValueError(
            f"No per-metric distance function for similarity={similarity!r}; "
            f"known: {sorted(_DISTANCE_FUNC_MAP)}"
        )
    return func


def _distance_expr(similarity, vector_field, qvec_var="qvec", record_var="m"):
    """Build a 2-arg brute-force distance SQL++ expression via the public
    vector_distance(a, b, metric) sugar (avoids private cosine_distance/dot_distance)."""
    metric = _VECTOR_DISTANCE_METRIC_MAP.get(similarity)
    if metric is None:
        raise ValueError(
            f"No vector_distance metric for similarity={similarity!r}; "
            f"known: {sorted(_VECTOR_DISTANCE_METRIC_MAP)}"
        )
    return f'vector_distance({record_var}.{vector_field}, {qvec_var}, "{metric}")'


def parse_time_ms(time_str):
    """Parse AsterixDB time strings ('1.234s' or '123.456ms') to float ms."""
    if not time_str:
        return 0.0
    time_str = time_str.strip()
    if time_str.endswith("ms"):
        return float(time_str[:-2])
    elif time_str.endswith("s"):
        return float(time_str[:-1]) * 1000
    return 0.0


def prepare_limited_dataset(source_path, limit):
    """Create a limited subset file if it doesn't already exist.

    Returns the path to use for loading (original if limit is None).
    """
    if limit is None:
        return source_path

    limited_path = f"{source_path}.limited_{limit}"
    if os.path.exists(limited_path):
        print(f"  [info] Reusing existing limited file: {limited_path}")
        return limited_path

    print(f"  [info] Creating limited dataset ({limit} records)...")
    with open(source_path, "r") as infile, open(limited_path, "w") as outfile:
        for i, line in enumerate(infile):
            if i >= limit:
                break
            outfile.write(line)
    print(f"  [info] Limited dataset created: {limited_path}")
    return limited_path


def load_query_vector_from_file(dataset_path, pk_field, pk_value,
                                vector_field):
    """Read a query vector directly from a JSONL/JSON file."""
    with open(dataset_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get(pk_field) == pk_value:
                return record.get(vector_field)
    return None
