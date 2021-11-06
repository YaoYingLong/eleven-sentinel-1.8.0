/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
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
 */
package com.alibaba.csp.sentinel.dashboard.rule.nacos.getway;

import com.alibaba.csp.sentinel.adapter.gateway.common.api.ApiDefinition;
import com.alibaba.csp.sentinel.dashboard.datasource.entity.gateway.ApiDefinitionEntity;
import com.alibaba.csp.sentinel.dashboard.rule.DynamicRuleProvider;
import com.alibaba.csp.sentinel.dashboard.rule.nacos.NacosConfigUtil;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.api.config.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Fox
 */
@Component("gatewayApiRuleNacosProvider")
public class GatewayApiRuleNacosProvider implements DynamicRuleProvider<List<ApiDefinitionEntity>> {

    @Autowired
    private ConfigService configService;
    @Value(value = "${sentinel.nacos.config.refreshTimeoutMs:3000}")
    private Integer refreshTimeoutMs;

    @Override
    public List<ApiDefinitionEntity> getRules(String appName, String ip, Integer port) throws Exception {
        String rules = configService.getConfig(
                appName + NacosConfigUtil.GATEWAY_API_DATA_ID_POSTFIX,
                NacosConfigUtil.GROUP_ID, refreshTimeoutMs);
        if (StringUtil.isEmpty(rules)) {
            return new ArrayList<>();
        }
        List<ApiDefinition> list = JSON.parseArray(rules, ApiDefinition.class);
        return list.stream().map(rule ->
                ApiDefinitionEntity.fromApiDefinition(appName, ip, port, rule))
                .collect(Collectors.toList());
    }
}