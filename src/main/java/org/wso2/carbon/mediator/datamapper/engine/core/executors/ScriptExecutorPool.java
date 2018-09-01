package org.wso2.carbon.mediator.datamapper.engine.core.executors;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * override
 * @author zhangqing
 *
 */
public class ScriptExecutorPool {
	
	private BlockingQueue<Executor> executors;

	public ScriptExecutorPool(ScriptExecutorType executorType, int executorPoolSize) {
		this.executors = new LinkedBlockingQueue<Executor>();
		for (int i = 0; i < executorPoolSize; ++i) {
			Executor executor = createScriptExecutor(executorType);
			if (executor != null)
				this.executors.add(executor);
		}
	}

	private Executor createScriptExecutor(ScriptExecutorType executorType) {
		return new ScriptExecutor(executorType);
	}

	public Executor take() throws InterruptedException {
		return ((Executor) this.executors.take());
	}

	public void put(Executor executor) throws InterruptedException {
		this.executors.put(executor);
	}
	
	public int count(){
		return executors == null ? 0 : executors.size();
	}
}
