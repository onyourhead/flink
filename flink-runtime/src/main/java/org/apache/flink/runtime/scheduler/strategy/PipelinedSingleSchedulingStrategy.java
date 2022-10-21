/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedSingleSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final DeploymentOption deploymentOption = new DeploymentOption(false);

    public PipelinedSingleSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

    }
    @Override
    public void startScheduling() {
        final Set<ExecutionVertexID> executionVertices =
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());
        maybeScheduleVertices(executionVertices);
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        maybeScheduleVertices(verticesToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {}

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    private void maybeScheduleVertices(final Set<ExecutionVertexID> vertices) {
        if (vertices.isEmpty()) {
            return;
        }
        final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions =
                SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
                        schedulingTopology, vertices, id -> {
                            if(id.isNeedUpdateConsumer()) {
                                return new DeploymentOption(true);
                            } else {
                                return deploymentOption;
                            }
//                                return deploymentOption;
                        });

        schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
    }

    /** The factory for creating {@link PipelinedSingleSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedSingleSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
