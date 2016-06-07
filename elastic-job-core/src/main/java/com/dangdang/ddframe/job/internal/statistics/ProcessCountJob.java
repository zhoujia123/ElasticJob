/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.internal.statistics;

import com.dangdang.ddframe.job.api.JobConfiguration;
import com.dangdang.ddframe.job.internal.guarantee.GuaranteeService;
import com.dangdang.ddframe.job.internal.server.ServerService;
import com.dangdang.ddframe.reg.base.CoordinatorRegistryCenter;

/**
 * 统计处理数据数量的作业.
 * 
 * @author zhangliang
 * @version 1.0 zhoujia 修改统计重置  为分布式任务都完成后执行
 */
public final class ProcessCountJob implements Runnable {
    
    private final JobConfiguration jobConfiguration;
    
    private final ServerService serverService;

    private final GuaranteeService guaranteeService;
    
    public ProcessCountJob(final CoordinatorRegistryCenter coordinatorRegistryCenter, final JobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
        serverService = new ServerService(coordinatorRegistryCenter, jobConfiguration);
        guaranteeService = new GuaranteeService(coordinatorRegistryCenter, jobConfiguration);
    }
    
    @Override
    public void run() {
        String jobName = jobConfiguration.getJobName();
        serverService.persistProcessSuccessCount(ProcessCountStatistics.getProcessSuccessCount(jobName));
        serverService.persistProcessFailureCount(ProcessCountStatistics.getProcessFailureCount(jobName));
//        ProcessCountStatistics.reset(jobName);  // --v1.0
        // +v1.0 start
        if(guaranteeService.isAllCompleted()){
            ProcessCountStatistics.reset(jobName);
        }
        // +v1.0 end
    }
}
