package org.wso2.carbon.transports.sap.bapi.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.utils.xml.StringUtils;

import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

public class RFCMetaDataParser {
	
	private static Log log = LogFactory.getLog(RFCMetaDataParser.class);
	
	private static final Collection<String> DEFAULT_PATTERNS = Arrays.asList(
			"yyyy-MMM-dd HH:mm:ss", "yyyy-MMM-dd", "HH:mm:ss");

	public static void processMetaDataDocument(OMElement document, JCoFunction function) throws AxisFault {
		Iterator<?> itr = document.getChildElements();
		while (itr.hasNext()) {
			OMElement childElement = (OMElement) itr.next();
			processElement(childElement, function);
		}
	}

	public static String getBAPIRFCFucntionName(OMElement rootElement) throws AxisFault {
		String localName = rootElement.getLocalName();
		log.info("localName: " + localName);
		if (localName != null) {
			if (localName.equals("bapirfc")) {
				String rfcFunctionName = rootElement.getAttributeValue(RFCConstants.NAME_Q);
				if (rfcFunctionName != null) {
					return rfcFunctionName;
				}
				throw new AxisFault("BAPI/RFC function name is mandatory in meta data configuration");

			}

			throw new AxisFault("Invalid meta data root element.Found: " + localName + "" + ". Required:" + "bapirfc");

		}

		return null;
	}

	private static void processElement(OMElement element, JCoFunction function) throws AxisFault {
		String qname = element.getQName().toString();
		if (qname != null)
			if (qname.equals("import"))
				processImport(element, function);
			else if (qname.equals("tables"))
				processTables(element, function);
			else
				log.warn("Unknown meta data type tag :" + qname + " detected. "
						+ "This meta data element will be discarded!");
	}

	private static void processImport(OMElement element, JCoFunction function) throws AxisFault {
		Iterator<?> itr = element.getChildElements();
		while (itr.hasNext()) {
			OMElement childElement = (OMElement) itr.next();
			String qname = childElement.getQName().toString();
			String name = childElement.getAttributeValue(RFCConstants.NAME_Q);
			if (qname.equals("structure"))
				processStructure(childElement, function, name);
			else if (qname.equals("field"))
				processField(childElement, function, name);
			else
				log.warn("Unknown meta data type tag :" + qname + " detected. "
						+ "This meta data element will be discarded!");
		}
	}

	private static void processStructure(OMElement element, JCoFunction function, String strcutName) throws AxisFault {
		if (strcutName == null) {
			throw new AxisFault("A structure should have a name!");
		}
		JCoStructure jcoStrcture = function.getImportParameterList().getStructure(strcutName);
		log.debug("结构(下面):");
		log.debug("\r\n"+jcoStrcture);
		if (jcoStrcture != null) {
			Iterator<?> itr = element.getChildElements();
			boolean isRecordFound = false;
			while (itr.hasNext()) {
				OMElement childElement = (OMElement) itr.next();
				String qname = childElement.getQName().toString();
				if (qname.equals("field")) {
					String fieldName = childElement.getAttributeValue(RFCConstants.NAME_Q);
					String fieldValue = childElement.getText();
					String javaType = childElement.getAttributeValue(RFCConstants.TYPE_Q);
					for (JCoField field : jcoStrcture) {
						if ((fieldName != null) && (fieldName.equals(field.getName()))) {
							isRecordFound = true;
							field.setValue(parseObject(fieldValue, javaType));
							break;
						}
					}
					if (!(isRecordFound))
						throw new AxisFault("Invalid configuration! The field : " + fieldName + ""
								+ " did not find the the strcture : " + strcutName);
				} else {
					log.warn("Invalid meta data type element found : " + qname + " .This meta data "
							+ "type will be ignored");
				}
			}
		} else {
			log.error("Didn't find the specified structure : " + strcutName + " on the RFC"
					+ " repository. This structure will be ignored");
		}
	}

	private static void processField(OMElement element, JCoFunction function, String fieldName) throws AxisFault {
		if (fieldName == null) {
			throw new AxisFault("A field should have a name!");
		}
		String fieldValue = element.getText();
		if (fieldValue == null)
			return;
		function.getImportParameterList().setValue(fieldName, fieldValue);
	}

	private static void processTables(OMElement element, JCoFunction function) throws AxisFault {
		Iterator<?> itr = element.getChildElements();
		while (itr.hasNext()) {
			OMElement childElement = (OMElement) itr.next();
			String qname = childElement.getQName().toString();
			String tableName = childElement.getAttributeValue(RFCConstants.NAME_Q);
			if (qname.equals("table"))
				processTable(childElement, function, tableName);
			else
				log.warn("Invalid meta data type element found : " + qname + " .This meta data "
						+ "type will be ignored");
		}
	}

	private static void processTable(OMElement element, JCoFunction function, String tableName) throws AxisFault {
		JCoTable inputTable = function.getTableParameterList().getTable(tableName);
		log.debug("表数据:");
		log.debug("\r\n"+inputTable);
		if (inputTable == null) {
			throw new AxisFault("Input table :" + tableName + " does not exist");
		}
		Iterator<?> itr = element.getChildElements();
		while (itr.hasNext()) {
			OMElement childElement = (OMElement) itr.next();
			String qname = childElement.getQName().toString();
			String id = childElement.getAttributeValue(RFCConstants.ID_Q);
			if (qname.equals("row"))
				processRow(childElement, inputTable, id);
			else
				log.warn("Invalid meta data type element found : " + qname + " .This meta data "
						+ "type will be ignored");
		}
	}

	private static void processRow(OMElement element, JCoTable table, String id) throws AxisFault {
		table.appendRow();
		Iterator<?> itr = element.getChildElements();
		while (itr.hasNext()) {
			OMElement childElement = (OMElement) itr.next();
			String qname = childElement.getQName().toString();
			if ((qname != null) && (qname.equals("field")))
				processField(childElement, table);
			else
				log.warn("Invalid meta data type element found : " + qname + " .This meta data "
						+ "type will be ignored");
		}
	}

	private static void processField(OMElement element, JCoTable table) throws AxisFault {
		String fieldName = element.getAttributeValue(RFCConstants.NAME_Q);
		String javaType = element.getAttributeValue(RFCConstants.TYPE_Q);
		String fieldValue = element.getText();
		log.debug("字段-fieldName: " + fieldName);
		log.debug("值-fieldValue: " + fieldValue);
		if (fieldName == null) {
			throw new AxisFault("A field should have a name!");
		}
		if (fieldValue != null)
			table.setValue(fieldName, parseObject(fieldValue, javaType));
	}
	
	/**
	 * 数据格式转换
	 * @param fieldValue
	 * @param javaType
	 * @return
	 */
	private static Object parseObject(String fieldValue, String javaType){
		if(StringUtils.isEmpty(javaType)){
			return fieldValue;
		}
		Object obj = fieldValue;
		try {
			if(javaType.equals("java.util.Date")){
				obj = DateUtil.parseDate(fieldValue, DEFAULT_PATTERNS);
			}else if(javaType.equals("java.lang.Integer")){
				obj = Integer.parseInt(fieldValue);
			}else if(javaType.equals("java.lang.Long")){
				obj = Long.parseLong(fieldValue);
			}else if(javaType.equals("java.lang.Float")){
				obj = Float.parseFloat(fieldValue);
			}
		} catch (Exception e) {
			log.warn("Invalid java type element found : " + javaType + ", transform failed .This meta data "
					+ "type will be ignored", e);
		}
		return obj;
	}
}
