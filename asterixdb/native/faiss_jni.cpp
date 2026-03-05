/*
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
 */
#include <jni.h>
#include <faiss/utils/distances.h>
#include <faiss/Clustering.h>
#include <faiss/IndexFlat.h>
#include <vector>
#include <cmath>


extern "C"
JNIEXPORT jfloat JNICALL Java_org_apache_asterix_runtime_utils_FaissWrapper_l2sqr
  (JNIEnv* env, jclass clazz, jfloatArray a, jfloatArray b) {
    jsize len = env->GetArrayLength(a);
    jfloat* pa = env->GetFloatArrayElements(a, nullptr);
    jfloat* pb = env->GetFloatArrayElements(b, nullptr);

    float dist = faiss::fvec_L2sqr(pa, pb, len);

    env->ReleaseFloatArrayElements(a, pa, JNI_ABORT);
    env->ReleaseFloatArrayElements(b, pb, JNI_ABORT);

    return dist;
}

extern "C"
JNIEXPORT jfloatArray JNICALL
Java_org_apache_asterix_runtime_utils_FaissWrapper_trainAndGetCentroids(
    JNIEnv* env,
    jclass clazz,
    jint d,             // dimension
    jint k,             // number of centroids
    jfloatArray jData,  // flattened input data array (size n * d)
    jint n              // number of vectors
) {
    // Step 1: Access data from Java
    jfloat* data = env->GetFloatArrayElements(jData, nullptr);
    if (data == nullptr) {
        return nullptr; // OOM or bad input
    }

    // Step 2: Set up FAISS KMeans
    faiss::ClusteringParameters cp;
    cp.niter = 20;  // default number of iterations

    faiss::Clustering clus(d, k, cp);
    faiss::IndexFlatL2 index(d);  // you can change this to IndexFlatIP for dot product

    clus.train(n, data, index);

    // Step 3: Extract centroids
    const std::vector<float>& centroids = clus.centroids;

    jsize numCentroids = k * d;
    jfloatArray jCentroids = env->NewFloatArray(numCentroids);
    if (jCentroids == nullptr) {
        env->ReleaseFloatArrayElements(jData, data, JNI_ABORT);
        return nullptr; // OOM
    }

    env->SetFloatArrayRegion(jCentroids, 0, numCentroids, centroids.data());

    // Step 4: Release resources
    env->ReleaseFloatArrayElements(jData, data, JNI_ABORT);  // JNI_ABORT avoids copying back

    return jCentroids;
}
