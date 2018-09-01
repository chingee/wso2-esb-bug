# wso2-esb-bug

> 该项目处理了一些wos2esb的5.0.0

> 依赖包: $ESB_HOME/repository/components/plugins

1. 处理了datamapper引擎包的JS引擎池在出错后回收丢失的问题,而导致初始化的资源耗尽(这个问题在ei的6.3.0解决)

- org.wso2.carbon.mediator.datamapper.engine_4.6.6.jar

> 主要修改了org.wso2.carbon.mediator.datamapper.engine.core.mapper.MappingHandler
	
2. 处理了SAP适配器对于table的不支持, 以及有时候取xml根节点取不到的问题

> payloadfactory会对xml自动追加namespace
	
- org.wso2.carbon.transports.sap_1.0.0.jar
