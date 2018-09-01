package org.wso2.carbon.mediator.datamapper.engine.core.executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * override
 * @author zhangqing
 *
 */
public class ScriptExecutorFactory {
	
	private static final Log log = LogFactory.getLog(ScriptExecutorFactory.class);
	private static ScriptExecutorPool executorPool = null;
	private static ScriptExecutorType scriptExecutorType = ScriptExecutorType.NASHORN;

	public static Executor getScriptExecutor(String executorPoolSize) throws InterruptedException {
		if (executorPool == null) {
			initializeExecutorPool(executorPoolSize);
		}
		return executorPool.take();
	}

	private static synchronized void initializeExecutorPool(String executorPoolSizeStr) {
		if (executorPool == null) {
			String javaVersion = System.getProperty("java.version");
			if ((javaVersion.startsWith("1.7")) || (javaVersion.startsWith("1.6"))) {
				scriptExecutorType = ScriptExecutorType.RHINO;
				log.debug("Script Engine set to Rhino");
			} else {
				log.debug("Script Engine set to Nashorn");
			}

			int executorPoolSize = 20;
			if (executorPoolSizeStr != null) {
				executorPoolSize = Integer.parseInt(executorPoolSizeStr);
				log.debug("Script executor pool size set to " + executorPoolSize);
			} else {
				log.debug("Using default script executor pool size " + executorPoolSize);
			}
			executorPool = new ScriptExecutorPool(scriptExecutorType, executorPoolSize);
		}
	}

	public static void releaseScriptExecutor(Executor executor) throws InterruptedException {
		executorPool.put(executor);
	}
}
