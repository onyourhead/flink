package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.HashSet;
import java.util.Set;

/**
 * @author peizhouyu
 * @packageName org.apache.flink.runtime.executiongraph.failover.flip1
 * @description:
 * @date 2021/4/12  11:16
 */
public class RestartPipelinedSinlgeFailoverStrategy implements FailoverStrategy{

    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause) {
        Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
        executionVertexId.setNeedUpdateConsumer(true);
        tasksToRestart.add(executionVertexId);
        return tasksToRestart;
    }

    public static class Factory implements FailoverStrategy.Factory {
        @Override
        public FailoverStrategy create(
                SchedulingTopology topology,
                ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {
            return new RestartPipelinedSinlgeFailoverStrategy();
        }
    }
}
