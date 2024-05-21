package master

import (
	dolphinv1alpha1 "github.com/zncdatadev/dolphinscheduler-operator/api/v1alpha1"
	"github.com/zncdatadev/dolphinscheduler-operator/internal/common"
	"github.com/zncdatadev/dolphinscheduler-operator/pkg/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewMasterLogging(
	scheme *runtime.Scheme,
	instance *dolphinv1alpha1.DolphinschedulerCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *dolphinv1alpha1.MasterRoleGroupSpec) *resource.LoggingRecociler[*dolphinv1alpha1.DolphinschedulerCluster, any] {
	if mergedCfg.Config.Logging == nil {
		return nil
	}
	if loggingSpec := mergedCfg.Config.Logging.Logging; loggingSpec == nil {
		return nil
	}
	loggingContentGenerator := getLoggingDataBuilder(mergedCfg.Config.Logging.Logging)
	logDataBuilder := resource.NewGenericRoleLoggingDataBuilder(getRole(), masterLogbackTpl,
		dolphinv1alpha1.LogbackPropertiesFileName, loggingContentGenerator)
	return resource.NewLoggingReconciler(scheme, instance, client, groupName, labels, mergedCfg, logDataBuilder,
		getRole(), logbackConfigMapName(instance.GetName(), groupName))
}

func getLoggingDataBuilder(loggingSpec *dolphinv1alpha1.LoggingConfigSpec) resource.LoggingContentGenerator {
	return common.TransformApiLogger(loggingSpec)
}

const masterLogbackTpl = `<?xml version="1.0" encoding="UTF-8"?>
<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<configuration scan="true" scanPeriod="120 seconds">
    <property name="log.base" value="logs"/>
    <property scope="context" name="log.base.ctx" value="${log.base}" />

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    {{- if .Console}}
        <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>{{.Console.Level}}</level>
        </filter>
    {{- end}}
        <encoder>
            <pattern>
                [WI-%X{workflowInstanceId:-0}][TI-%X{taskInstanceId:-0}] - [%level] %date{yyyy-MM-dd HH:mm:ss.SSS Z} %logger{10}:[%line] - %msg%n
            </pattern>
            <charset>UTF-8</charset>
        </encoder>
    </appender>

    <conversionRule conversionWord="message"
                    converterClass="org.apache.dolphinscheduler.common.log.SensitiveDataConverter"/>
    <appender name="TASKLOGFILE" class="ch.qos.logback.classic.sift.SiftingAppender">
    {{- if .File}}
        <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
            <level>{{.File.Level}}</level>
        </filter>
    {{- end}}
        <filter class="org.apache.dolphinscheduler.plugin.task.api.log.TaskLogFilter"/>
        <Discriminator class="org.apache.dolphinscheduler.plugin.task.api.log.TaskLogDiscriminator">
            <key>taskInstanceLogFullPath</key>
            <logBase>${log.base}</logBase>
        </Discriminator>
        <sift>
            <appender name="FILE-${taskInstanceLogFullPath}" class="ch.qos.logback.core.FileAppender">
                <file>${taskInstanceLogFullPath}</file>
                <encoder>
                    <pattern>
                        [%level] %date{yyyy-MM-dd HH:mm:ss.SSS Z} - %message%n
                    </pattern>
                    <charset>UTF-8</charset>
                </encoder>
                <append>true</append>
            </appender>
        </sift>
    </appender>
    <appender name="MASTERLOGFILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${log.base}/dolphinscheduler-master.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy">
            <fileNamePattern>${log.base}/dolphinscheduler-master.%d{yyyy-MM-dd_HH}.%i.log</fileNamePattern>
            <maxHistory>168</maxHistory>
            <maxFileSize>200MB</maxFileSize>
            <totalSizeCap>50GB</totalSizeCap>
            <cleanHistoryOnStart>true</cleanHistoryOnStart>
        </rollingPolicy>
        <encoder>
            <pattern>
                [WI-%X{workflowInstanceId:-0}][TI-%X{taskInstanceId:-0}] - [%level] %date{yyyy-MM-dd HH:mm:ss.SSS Z} %logger{10}:[%line] - %msg%n
            </pattern>
            <charset>UTF-8</charset>
        </encoder>
    </appender>

{{- range .Loggers}}
    <logger name="{{.Logger}}" level="{{.Level}}"/>
{{- end}}

    <root level="INFO">
        <if condition="${DOCKER:-false}">
            <then>
                <appender-ref ref="STDOUT"/>
            </then>
        </if>
        <appender-ref ref="TASKLOGFILE"/>
        <appender-ref ref="MASTERLOGFILE"/>
    </root>
</configuration>
`
