package com.eleven.sentinel.extension.filepull;

import com.alibaba.csp.sentinel.command.handler.ModifyParamFlowRulesCommandHandler;
import com.alibaba.csp.sentinel.datasource.FileRefreshableDataSource;
import com.alibaba.csp.sentinel.datasource.FileWritableDataSource;
import com.alibaba.csp.sentinel.datasource.ReadableDataSource;
import com.alibaba.csp.sentinel.datasource.WritableDataSource;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.system.SystemRule;
import com.alibaba.csp.sentinel.slots.system.SystemRuleManager;
import com.alibaba.csp.sentinel.transport.util.WritableDataSourceRegistry;

import java.io.FileNotFoundException;
import java.util.List;

/**
 * @author by YingLong on 2021/11/5
 */
public class FileDataSourceInit implements InitFunc {
    @Override
    public void init() throws Exception {
        RuleFileUtils.mkdirIfNotExits(PersistenceRuleConstant.STORE_PATH); // 创建文件存储目录
        RuleFileUtils.createFileIfNotExits(PersistenceRuleConstant.RULES_MAP); // 创建规则文件
        dealFlowRules(); // 处理流控规则逻辑  配置读写数据源
        dealDegradeRules(); // 处理降级规则
        dealSystemRules(); // 处理系统规则
        dealParamFlowRules(); // 处理热点参数规则
        dealAuthRules(); // 处理授权规则
    }

    private void dealAuthRules() throws FileNotFoundException {
        String ruleFilePath = PersistenceRuleConstant.RULES_MAP.get(PersistenceRuleConstant.AUTH_RULE_PATH).toString();
        ReadableDataSource<String, List<AuthorityRule>> flowRuleDataSource = new FileRefreshableDataSource(ruleFilePath, RuleListConverterUtils.authorityRuleParse);
        AuthorityRuleManager.register2Property(flowRuleDataSource.getProperty());
        WritableDataSource<List<AuthorityRule>> flowRuleWDS = new FileWritableDataSource<List<AuthorityRule>>(
                ruleFilePath, RuleListConverterUtils.authorityEncoding
        );
        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerAuthorityDataSource(flowRuleWDS);
    }

    private void dealParamFlowRules() throws FileNotFoundException {
        String ruleFilePath = PersistenceRuleConstant.RULES_MAP.get(PersistenceRuleConstant.HOT_PARAM_RULE).toString();
        ReadableDataSource<String, List<ParamFlowRule>> flowRuleDataSource = new FileRefreshableDataSource(ruleFilePath, RuleListConverterUtils.paramFlowRuleListParse);
        ParamFlowRuleManager.register2Property(flowRuleDataSource.getProperty());

        WritableDataSource<List<ParamFlowRule>> flowRuleWDS = new FileWritableDataSource<List<ParamFlowRule>>(
                ruleFilePath, RuleListConverterUtils.paramRuleEnCoding
        );
        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        ModifyParamFlowRulesCommandHandler.setWritableDataSource(flowRuleWDS);
    }

    private void dealSystemRules() throws FileNotFoundException {
        String ruleFilePath = PersistenceRuleConstant.RULES_MAP.get(PersistenceRuleConstant.DEGRAGE_RULE_PATH).toString();
        ReadableDataSource<String, List<SystemRule>> flowRuleDataSource = new FileRefreshableDataSource(ruleFilePath, RuleListConverterUtils.sysRuleListParse);
        SystemRuleManager.register2Property(flowRuleDataSource.getProperty());

        WritableDataSource<List<SystemRule>> flowRuleWDS = new FileWritableDataSource<List<SystemRule>>(
                ruleFilePath, RuleListConverterUtils.sysRuleEnCoding
        );
        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerSystemDataSource(flowRuleWDS);

    }

    private void dealDegradeRules() throws FileNotFoundException {
        String ruleFilePath = PersistenceRuleConstant.RULES_MAP.get(PersistenceRuleConstant.DEGRAGE_RULE_PATH).toString();
        ReadableDataSource<String, List<DegradeRule>> ruleDataSource = new FileRefreshableDataSource(ruleFilePath, RuleListConverterUtils.degradeRuleListParse);
        DegradeRuleManager.register2Property(ruleDataSource.getProperty());

        WritableDataSource<List<DegradeRule>> ruleWDS = new FileWritableDataSource<List<DegradeRule>>(
                ruleFilePath, RuleListConverterUtils.degradeRuleEnCoding
        );
        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerDegradeDataSource(ruleWDS);
    }

    private void dealFlowRules() throws FileNotFoundException {
        String ruleFilePath = PersistenceRuleConstant.RULES_MAP.get(PersistenceRuleConstant.FLOW_RULE_PATH).toString();
        ReadableDataSource<String, List<FlowRule>> flowRuleDataSource = new FileRefreshableDataSource(ruleFilePath, RuleListConverterUtils.flowRuleListParser);
        FlowRuleManager.register2Property(flowRuleDataSource.getProperty());

        WritableDataSource<List<FlowRule>> flowRuleWDS = new FileWritableDataSource<List<FlowRule>>(
                ruleFilePath, RuleListConverterUtils.flowFuleEnCoding
        );
        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerFlowDataSource(flowRuleWDS);
    }
}
