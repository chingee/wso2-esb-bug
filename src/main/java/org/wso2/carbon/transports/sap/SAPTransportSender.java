package org.wso2.carbon.transports.sap;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.OutInAxisOperation;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportOutDescription;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.transport.OutTransportInfo;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.base.AbstractTransportSender;
import org.apache.axis2.util.MessageContextBuilder;
import org.wso2.carbon.transports.sap.bapi.util.RFCMetaDataParser;
import org.wso2.carbon.transports.sap.idoc.DefaultIDocXMLMapper;
import org.wso2.carbon.transports.sap.idoc.IDocXMLMapper;

import com.sap.conn.idoc.IDocDocumentList;
import com.sap.conn.idoc.IDocRepository;
import com.sap.conn.idoc.jco.JCoIDoc;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.ext.Environment;

public class SAPTransportSender extends AbstractTransportSender {
	private Map<String, IDocXMLMapper> xmlMappers;
	private IDocXMLMapper defaultMapper;
	public static final String ERROR_CODE = "ERROR_CODE";
	public static final int SAP_TRANSPORT_ERROR = 8000;
	public static final int SAP_DESTINATION_ERROR = 8001;

	public SAPTransportSender() {
		this.xmlMappers = new HashMap<String, IDocXMLMapper>();

		this.defaultMapper = new DefaultIDocXMLMapper();
	}

	public void init(ConfigurationContext cfgCtx, TransportOutDescription trpOut) throws AxisFault {
		super.init(cfgCtx, trpOut);

		CarbonDestinationDataProvider provider = new CarbonDestinationDataProvider();
		if (!(Environment.isServerDataProviderRegistered())) {
			Environment.registerServerDataProvider(provider);
		}
		if (!(Environment.isDestinationDataProviderRegistered())) {
			Environment.registerDestinationDataProvider(provider);
		}

		Parameter xmlMappersParam = trpOut.getParameter("transport.sap.customXMLMappers");
		if (xmlMappersParam != null) {
			OMElement mappersElt = xmlMappersParam.getParameterElement().getFirstElement();
			Iterator<?> mappers = mappersElt.getChildrenWithName(new QName("mapper"));
			try {
				while (mappers.hasNext()) {
					OMElement m = (OMElement) mappers.next();
					String key = m.getAttributeValue(new QName("key"));
					String value = m.getText().trim();
					Class<?> clazz = super.getClass().getClassLoader().loadClass(value);
					IDocXMLMapper mapper = (IDocXMLMapper) clazz.newInstance();

					this.xmlMappers.put(key, mapper);
				}
			} catch (Exception e) {
				throw new AxisFault("Error while initializing the SAP transport sender", e);
			}
		}
	}

	public void sendMessage(MessageContext messageContext, String targetEPR, OutTransportInfo outTransportInfo)
			throws AxisFault {
		if (targetEPR == null) {
			throw new AxisFault("Cannot send an IDoc without a target SAP EPR");
		}
		try {
			URI uri = new URI(targetEPR);
			String destName = uri.getPath().substring(1);
			JCoDestination destination = JCoDestinationManager.getDestination(destName);

			if (uri.getScheme().equals("idoc")) {
				IDocRepository iDocRepository = JCoIDoc.getIDocRepository(destination);
				String tid = destination.createTID();
				IDocDocumentList iDocList = getIDocs(messageContext, iDocRepository);
				JCoIDoc.send(iDocList, getIDocVersion(uri), destination, tid);
				destination.confirmTID(tid);
			} else if (uri.getScheme().equals("bapi")) {
				try {
					OMElement body = messageContext.getEnvelope().getBody();
					OMElement payLoad = body.getFirstElement();
					this.log.info("Received RFC/Meta DATA: " + payLoad);
					String rfcFunctionName = RFCMetaDataParser.getBAPIRFCFucntionName(payLoad);
					this.log.info("Looking up the BAPI/RFC function: " + rfcFunctionName + ". In the "
							+ "meta data repository");

					JCoFunction function = getRFCfunction(destination, rfcFunctionName);
					RFCMetaDataParser.processMetaDataDocument(payLoad, function);
					String responseXML = evaluateRFCfunction(function, destination);
					processResponse(messageContext, responseXML);
				} catch (Exception e) {
					this.log.error("发送数据到SAP:" + targetEPR + "出错", e);
					sendFault(messageContext, e, 8000);
				}
			} else {
				handleException("Invalid protocol name : " + uri.getScheme() + " in SAP URL");
			}
		} catch (Exception e) {
			this.log.error("发送数据到SAP:" + targetEPR + "出错", e);
			sendFault(messageContext, e, 8001);
			handleException("Error while sending an IDoc to the EPR : " + targetEPR, e);
		}
	}

	private char getIDocVersion(URI uri) {
		String query = uri.getQuery();
		if ((query != null) && (query.startsWith("version"))) {
			String version = query.substring(query.indexOf(61) + 1);
			if ("2".equals(version))
				return '2';
			if ("3".equals(version)) {
				return '3';
			}
		}
		return '0';
	}

	private IDocDocumentList getIDocs(MessageContext msgContext, IDocRepository repo) throws Exception {
		Object mapper = msgContext.getOptions().getProperty("transport.sap.xmlMapper");

		if ((mapper != null) && (this.xmlMappers.containsKey(mapper.toString()))) {
			return ((IDocXMLMapper) this.xmlMappers.get(mapper.toString())).getDocumentList(repo, msgContext);
		}
		return this.defaultMapper.getDocumentList(repo, msgContext);
	}

	private String evaluateRFCfunction(JCoFunction function, JCoDestination destination) throws AxisFault {
		this.log.info("Invoking the RFC function :" + function.getName());
		try {
			function.execute(destination);
		} catch (JCoException e) {
			throw new AxisFault("Cloud not execute the RFC function: " + function, e);
		}

		JCoStructure returnStructure = null;
		try {
			returnStructure = function.getExportParameterList().getStructure("RETURN");
		} catch (Exception ignore) {
		}
		if ((returnStructure != null) && (!(returnStructure.getString("TYPE").equals("")))
				&& (!(returnStructure.getString("TYPE").equals("S")))) {
			throw new AxisFault(returnStructure.getString("MESSAGE"));
		}

		return function.toXML();
	}

	private JCoFunction getRFCfunction(JCoDestination destination, String rfcName) throws AxisFault {
		this.log.info("Retriving the BAPI/RFC function : " + rfcName + " from the destination : " + destination);

		JCoFunction function = null;
		try {
			function = destination.getRepository().getFunction(rfcName);
		} catch (JCoException e) {
			throw new AxisFault("RFC function " + function + " cloud not found in SAP system", e);
		}
		return function;
	}

	private void processResponse(MessageContext msgContext, String payLoad) throws AxisFault {
		if (!(msgContext.getAxisOperation() instanceof OutInAxisOperation))
			return;
		try {
			MessageContext responseMessageContext = createResponseMessageContext(msgContext);
			ByteArrayInputStream bais = new ByteArrayInputStream(payLoad.getBytes());
			SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext, bais, "application/xml");

			responseMessageContext.setEnvelope(envelope);
			AxisEngine.receive(responseMessageContext);
			this.log.info("Sending response out..");
		} catch (XMLStreamException e) {
			throw new AxisFault("Error while processing response", e);
		}
	}

	private void sendFault(MessageContext msgContext, Exception e, int errorCode) {
		try {
			MessageContext faultContext = MessageContextBuilder.createFaultMessageContext(msgContext, e);

			faultContext.setProperty("ERROR_CODE", Integer.valueOf(errorCode));
			faultContext.setProperty("ERROR_MESSAGE", e.getMessage());
			faultContext.setProperty("SENDING_FAULT", Boolean.TRUE);
			msgContext.getAxisOperation().getMessageReceiver().receive(faultContext);
		} catch (AxisFault axisFault) {
			this.log.fatal("Cloud not create the fault message.", axisFault);
		}
	}
}