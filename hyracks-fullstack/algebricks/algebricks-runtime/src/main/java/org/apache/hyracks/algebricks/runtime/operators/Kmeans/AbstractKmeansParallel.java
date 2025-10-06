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
package org.apache.hyracks.algebricks.runtime.operators.Kmeans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractKmeansParallel<T extends IRunningAggregateEvaluator>
        extends AbstractOneInputOneOutputOneFramePushRuntime {

    protected final IEvaluatorContext ctx;
    private final IRunningAggregateEvaluatorFactory[] runningAggFactories;
    private final Class<T> runningAggEvalClass;
    protected final List<T> runningAggEvals;
    private final int[] projectionColumns;
    private final int[] projectionToOutColumns;
    private final IPointable p = VoidPointable.FACTORY.createPointable();
    protected ArrayTupleBuilder tupleBuilder;
    private boolean isFirst;

    public AbstractKmeansParallel(int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, Class<T> runningAggEvalClass,
            IHyracksTaskContext ctx) {
        this.ctx = new EvaluatorContext(ctx);
        this.projectionColumns = projectionColumns;
        this.runningAggFactories = runningAggFactories;
        this.runningAggEvalClass = runningAggEvalClass;
        runningAggEvals = new ArrayList<>(runningAggFactories.length);
        projectionToOutColumns = new int[projectionColumns.length];
        for (int j = 0; j < projectionColumns.length; j++) {
            projectionToOutColumns[j] = Arrays.binarySearch(runningAggOutColumns, projectionColumns[j]);
        }
        isFirst = true;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        if (isFirst) {
            isFirst = false;
            init();
        }
        for (T runningAggEval : runningAggEvals) {
            runningAggEval.init();
        }
    }

    protected void init() throws HyracksDataException {
        tupleBuilder = createOutputTupleBuilder(projectionColumns);
        initAccessAppendRef(ctx.getTaskContext());
        for (IRunningAggregateEvaluatorFactory runningAggFactory : runningAggFactories) {
            IRunningAggregateEvaluator runningAggEval = runningAggFactory.createRunningAggregateEvaluator(ctx);
            runningAggEvals.add(runningAggEvalClass.cast(runningAggEval));
        }
    }

    protected void kmeansinit() {
        Random baseRand = new Random();
        long seed = baseRand.nextInt();
        List<double[]> data = new ArrayList<>();
        int n = data.size();
        int k = 5; // number of clusters
        if (n == 0)
            throw new IllegalArgumentException("No samples available from data.");
        double[] costs = new double[n];
        Arrays.fill(costs, Double.POSITIVE_INFINITY);
        int initializationSteps = 5;

        Random rand = new Random(seed);
        // Choose one random initial center
        double[] firstCenter = data.get(rand.nextInt(n));
        List<double[]> centers = new ArrayList<>();
        centers.add(firstCenter);

        List<double[]> newCenters = new ArrayList<>();
        newCenters.add(firstCenter);

        for (int step = 0; step < initializationSteps; step++) {
            // WAIT FOR SIGNAL from elsewhere before proceeding to the next initialization step.
            // For example, use a CountDownLatch, wait/notify, or other synchronization primitive.
            // signal.await(); // <-- placeholder for actual waiting logic

            // Update costs: for each point, keep minimum squared distance to any new center
            for (int i = 0; i < n; i++) {
                for (double[] center : newCenters) {
                    //                    double d = distanceFunction.distance(data.get(i), center);
                    //                    if (d < costs[i]) costs[i] = d;
                    //                }
                }
                double sumCosts = Arrays.stream(costs).sum();

                // Choose new centers: for each point, with prob ~ 2*k*cost/sumCosts, add as center
                List<double[]> chosen = new ArrayList<>();
                for (int j = 0; j < n; j++) {
                    double prob = 2.0 * k * costs[j] / sumCosts;
                    if (rand.nextDouble() < prob) {
                        chosen.add(data.get(j));
                    }
                }
                // Prepare for next round
                newCenters = chosen;
                centers.addAll(chosen);

                // Pass to next operator
            }
        }
    }

    protected ArrayTupleBuilder createOutputTupleBuilder(int[] projectionList) {
        return new ArrayTupleBuilder(projectionList.length);
    }

    protected void produceTuples(IFrameTupleAccessor accessor, int beginIdx, int endIdx, FrameTupleReference tupleRef)
            throws HyracksDataException {
        for (int t = beginIdx; t <= endIdx; t++) {
            tupleRef.reset(accessor, t);
            produceTuple(tupleBuilder, accessor, t, tupleRef);
            appendToFrameFromTupleBuilder(tupleBuilder);
        }
    }

    protected void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
            FrameTupleReference tupleRef) throws HyracksDataException {
        tb.reset();
        for (int f = 0; f < projectionColumns.length; f++) {
            int k = projectionToOutColumns[f];
            if (k >= 0) {
                runningAggEvals.get(k).step(tupleRef, p);
                tb.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
            } else {
                tb.addField(accessor, tIndex, projectionColumns[f]);
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }
}
