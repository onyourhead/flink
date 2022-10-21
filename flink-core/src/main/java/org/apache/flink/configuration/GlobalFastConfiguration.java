package org.apache.flink.configuration;

import org.apache.flink.util.StringUtils;

/**
 * @author zhangzhengqi5
 */

public enum GlobalFastConfiguration {
    /**
     * single instance
     */
    INSTANCE;

    private Configuration flinkConfig = new Configuration();

    public void setFlinkConfig(Configuration flinkConfig) {
        if (flinkConfig != null){
            this.flinkConfig = flinkConfig;
        }
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    /**
     * apus individual
     */
    public static final String SINGLE_TASK_RECOVER = "single";

	//---------------------------------------------------------------------------------
	// help methods
	//---------------------------------------------------------------------------------

    /**
     * apus individual is enabled
     * @return true or false
     */
    public boolean isSingleTaskRecover(){
        String failoverStrategy = flinkConfig.getString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY);
        return !StringUtils.isNullOrWhitespaceOnly(failoverStrategy)
                && failoverStrategy.equalsIgnoreCase(
                SINGLE_TASK_RECOVER);
    }
}
