package org.wso2.carbon.mediator.datamapper.engine.core.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.JSException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.ReaderException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.SchemaException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.WriterException;
import org.wso2.carbon.mediator.datamapper.engine.core.executors.Executor;
import org.wso2.carbon.mediator.datamapper.engine.core.executors.ScriptExecutorFactory;
import org.wso2.carbon.mediator.datamapper.engine.core.models.Model;
import org.wso2.carbon.mediator.datamapper.engine.core.notifiers.InputVariableNotifier;
import org.wso2.carbon.mediator.datamapper.engine.core.notifiers.OutputVariableNotifier;
import org.wso2.carbon.mediator.datamapper.engine.input.InputBuilder;
import org.wso2.carbon.mediator.datamapper.engine.output.OutputMessageBuilder;
import org.wso2.carbon.mediator.datamapper.engine.utils.InputOutputDataType;
import org.wso2.carbon.mediator.datamapper.engine.utils.ModelType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * override
 * @author zhangqing
 *
 */
public class MappingHandler implements InputVariableNotifier, OutputVariableNotifier {
	
	private static final Log log = LogFactory.getLog(MappingHandler.class);
	
	private String dmExecutorPoolSize;
	private String inputVariable;
	private String outputVariable;
	private MappingResource mappingResource;
	private OutputMessageBuilder outputMessageBuilder;
	private InputBuilder inputBuilder;
	private String propertiesInJSON;

	public MappingHandler(MappingResource mappingResource, String inputType, String outputType,
			String dmExecutorPoolSize) throws IOException, SchemaException, WriterException {
		this.inputBuilder = new InputBuilder(InputOutputDataType.fromString(inputType),
				mappingResource.getInputSchema());

		this.outputMessageBuilder = new OutputMessageBuilder(InputOutputDataType.fromString(outputType),
				ModelType.JAVA_MAP, mappingResource.getOutputSchema());

		this.dmExecutorPoolSize = dmExecutorPoolSize;
		this.mappingResource = mappingResource;
	}

	public String doMap(InputStream inputMsg, Map<String, Map<String, Object>> propertiesMap)
			throws ReaderException, InterruptedException, IOException, SchemaException, JSException {
		this.propertiesInJSON = propertiesMapToJSON(propertiesMap);
		this.inputBuilder.buildInputModel(inputMsg, this);
		return this.outputVariable;
	}

	@SuppressWarnings("rawtypes")
	public void notifyInputVariable(Object variable) throws SchemaException, JSException, ReaderException {
		this.inputVariable = (String) variable;
		Executor scriptExecutor = null;
		try{
			scriptExecutor = ScriptExecutorFactory.getScriptExecutor(this.dmExecutorPoolSize);
			Model outputModel = scriptExecutor.execute(this.mappingResource, this.inputVariable,
					this.propertiesInJSON);
			if (outputModel.getModel() instanceof Map) {
				this.outputMessageBuilder.buildOutputMessage(outputModel, (OutputVariableNotifier) this);
			} else {
				this.notifyOutputVariable(outputModel.getModel());
			}
		}catch (InterruptedException | WriterException e){
			throw new ReaderException(e.getMessage());
		} finally {
			if(scriptExecutor != null){
				try {
					this.releaseExecutor(scriptExecutor);
				} catch (InterruptedException e) {
					log.info("Error while releaseScriptExecutor", e);
					e.printStackTrace();
				}
			}
		}
	}

	private void releaseExecutor(Executor scriptExecutor) throws InterruptedException {
		log.debug("start releaseScriptExecutor");
		ScriptExecutorFactory.releaseScriptExecutor(scriptExecutor);
		scriptExecutor = null;
		log.debug("end releaseScriptExecutor");
	}

	public void notifyOutputVariable(Object variable) {
		this.outputVariable = ((String) variable);
	}

	private String propertiesMapToJSON(Map<String, Map<String, Object>> propertiesMap) throws ReaderException {
		ObjectMapper mapperObj = new ObjectMapper();
		String propertiesInJSON = null;
		try {
			propertiesInJSON = mapperObj.writeValueAsString(propertiesMap);
		} catch (JsonProcessingException e) {
			throw new ReaderException("Error while parsing the input properties. " + e.getMessage());
		}
		return propertiesInJSON;
	}
}
