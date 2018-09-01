package org.wso2.carbon.mediator.datamapper.engine.input.readers;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.OMXMLBuilderFactory;
import org.apache.axiom.om.OMXMLParserWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.InvalidPayloadException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.JSException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.ReaderException;
import org.wso2.carbon.mediator.datamapper.engine.core.exceptions.SchemaException;
import org.wso2.carbon.mediator.datamapper.engine.core.schemas.JacksonJSONSchema;
import org.wso2.carbon.mediator.datamapper.engine.core.schemas.Schema;
import org.wso2.carbon.mediator.datamapper.engine.input.InputBuilder;
import org.wso2.carbon.mediator.datamapper.engine.input.builders.JSONBuilder;

public class XMLInputReader implements InputReader

{

	private static final Log log = LogFactory.getLog(XMLInputReader.class);

	private InputBuilder messageBuilder;

	private Schema inputSchema;

	private String localName;

	private String nameSpaceURI;

	private JSONBuilder jsonBuilder;

	private Iterator<OMAttribute> it_attr;

	private Map jsonSchema;

	public XMLInputReader() throws IOException {

		this.jsonBuilder = new JSONBuilder();

	}

	public void read(final InputStream input, final Schema inputSchema, final InputBuilder messageBuilder)
			throws ReaderException {

		this.messageBuilder = messageBuilder;
		this.inputSchema = inputSchema;

		final OMXMLParserWrapper parserWrapper = OMXMLBuilderFactory.createOMBuilder(input);
		final OMElement root = parserWrapper.getDocumentElement();
		this.jsonSchema = this.getInputSchema().getSchemaMap();

		try {
			this.xmlTraverse(root, null, this.jsonSchema);
			this.jsonBuilder.writeEndObject();
		} catch (IOException | JSException | SchemaException | InvalidPayloadException e) {
			throw new ReaderException("Error while parsing XML input stream. " + e.getMessage());
		} finally {
			try {
				this.writeTerminateElement();
			} catch (IOException | JSException | SchemaException e) {
				e.printStackTrace();
				throw new ReaderException("Error while releaseScriptExecutor, please reboot the server" + e.getMessage());
			}
		}

	}

	public String xmlTraverse(final OMElement omElement, String prevElementName, final Map jsonSchemaMap)
			throws IOException, ReaderException, SchemaException, JSException, InvalidPayloadException {

		boolean isObject = false;

		boolean isArrayElement = false;

		String prevElementNameSpaceLocalName = null;

		this.localName = omElement.getLocalName();
		this.nameSpaceURI = this.getNameSpaceURI(omElement);
		final String nameSpaceLocalName = this.getNamespacesAndIdentifiersAddedFieldName(this.nameSpaceURI,
				this.localName, omElement);

		String elementType = this.getElementType(jsonSchemaMap, nameSpaceLocalName);
		if ("null".equals(elementType)) {

			XMLInputReader.log.warn((Object) ("Element name not found : " + nameSpaceLocalName));

		}
		final Map nextJSONSchemaMap = this.buildNextSchema(jsonSchemaMap, elementType, nameSpaceLocalName);
		if (nextJSONSchemaMap == null) {
			throw new ReaderException(
					"Input type is incorrect or Invalid element found in the message payload : " + nameSpaceLocalName);

		}

		if (prevElementName != null && !nameSpaceLocalName.equals(prevElementName)) {
			this.writeArrayEndElement();
			prevElementName = null;

		}
		if ("array".equals(elementType)) {
			if (prevElementName == null) {
				this.writeArrayStartElement(nameSpaceLocalName);
			}
			elementType = this.getArraySubElementType(jsonSchemaMap, nameSpaceLocalName);
			isArrayElement = true;

		}
		if (nameSpaceLocalName.equals(this.getInputSchema().getName())) {
			this.writeAnonymousObjectStartElement();
		} else if ("object".equals(elementType)) {
			isObject = true;
			if (isArrayElement) {
				this.writeAnonymousObjectStartElement();
				elementType = this.getArrayObjectTextElementType(jsonSchemaMap, nameSpaceLocalName);
			} else {
				this.writeObjectStartElement(nameSpaceLocalName);
				elementType = this.getObjectTextElementType(jsonSchemaMap, nameSpaceLocalName);

			}
		}
		if ("string".equals(elementType) || "boolean".equals(elementType) || "integer".equals(elementType)
				|| "number".equals(elementType)) {

			if (isObject) {
				this.writeFieldElement("_ELEMVAL", omElement.getText(), elementType);
			} else if (!isArrayElement) {
				this.writeFieldElement(nameSpaceLocalName, omElement.getText(), elementType);
			} else {
				this.writePrimitiveElement(omElement.getText(), elementType);

			}

		}
		this.it_attr = (Iterator<OMAttribute>) omElement.getAllAttributes();
		if (this.it_attr.hasNext()) {
			this.writeAttributes(nextJSONSchemaMap);

		}
		final Iterator<OMElement> it = (Iterator<OMElement>) omElement.getChildElements();

		if (!this.isXsiNil(omElement)) {
			while (it.hasNext()) {
				prevElementNameSpaceLocalName = this.xmlTraverse(it.next(), prevElementNameSpaceLocalName,
						nextJSONSchemaMap);

			}

		}

		if (prevElementNameSpaceLocalName != null) {
			this.writeArrayEndElement();

		}
		if (isObject) {
			this.writeObjectEndElement();

		}
		if (isArrayElement) {
			return nameSpaceLocalName;
		}
		return null;

	}

	private String getNameSpaceURI(final OMElement omElement) {

		String nameSpaceURI = "";
		if (omElement.getNamespace() != null) {
			nameSpaceURI = omElement.getNamespace().getNamespaceURI();
		}
		return nameSpaceURI;

	}

	private String getNameSpaceURI(final OMAttribute omAttribute) {

		String nameSpaceURI = "";
		if (omAttribute.getNamespace() != null) {
			nameSpaceURI = omAttribute.getNamespace().getNamespaceURI();
		}
		return nameSpaceURI;

	}

	private void writeAttributes(final Map jsonSchemaMap)
			throws JSException, SchemaException, ReaderException, IOException, InvalidPayloadException {

		while (this.it_attr.hasNext()) {
			final OMAttribute omAttribute = this.it_attr.next();

			final String attributeLocalName = omAttribute.getLocalName();
			if (attributeLocalName.contains("xmlns")) {
				continue;
			}
			final String attributeNSURI = this.getNameSpaceURI(omAttribute);
			final String attributeFieldName = this.getAttributeFieldName(attributeLocalName, attributeNSURI);
			final String attributeQName = this.getAttributeQName(omAttribute.getNamespace(), attributeLocalName);

			final String attributeType = this.getElementType(jsonSchemaMap, attributeQName);
			if ("null".equals(attributeType)) {

				XMLInputReader.log.warn((Object) ("Attribute name not found : " + attributeQName));

			}

			this.writeFieldElement(attributeFieldName, omAttribute.getAttributeValue(), attributeType);

		}

	}

	private String getElementType(Map jsonSchemaMap, String elementName) throws SchemaException {
		String elementType = "null";
		if (elementName.equals(getInputSchema().getName()))
			elementType = (String) jsonSchemaMap.get("type");
		else if (jsonSchemaMap.containsKey(elementName)) {
			elementType = (String) ((Map) jsonSchemaMap.get(elementName)).get("type");
		}
		return elementType;
	}

	private Map buildNextSchema(final Map jsonSchemaMap, final String elementType, final String elementName)
			throws SchemaException {
		Map nextSchema = null;

		if (elementName.equals(getInputSchema().getName()))
			nextSchema = (Map) jsonSchemaMap.get("properties");
		else if (jsonSchemaMap.containsKey(elementName)) {
			if ("array".equals(elementType)) {
				nextSchema = ((JacksonJSONSchema) this.inputSchema)
						.getSchemaItems((Map) jsonSchemaMap.get(elementName));

				nextSchema = getSchemaProperties(nextSchema);
			} else {
				nextSchema = getSchemaProperties((Map) jsonSchemaMap.get(elementName));
			}
		}
		return nextSchema;
	}

	private Map<String, Object> getSchemaProperties(Map<String, Object> schema) {
		Map nextSchema = new HashMap();
		if (schema.containsKey("properties")) {
			nextSchema.putAll((Map) schema.get("properties"));
		}
		if (schema.containsKey("attributes")) {
			nextSchema.putAll((Map) schema.get("attributes"));
		}
		return nextSchema;
	}

	private String getArraySubElementType(Map jsonSchemaMap, String elementName) {
		ArrayList itemsList = (ArrayList) ((Map) jsonSchemaMap.get(elementName)).get("items");
		String output = (String) ((Map) itemsList.get(0)).get("type");
		return output;
	}

	private String getArrayObjectTextElementType(Map jsonSchemaMap, String elementName) {
		ArrayList itemsList = (ArrayList) ((Map) jsonSchemaMap.get(elementName)).get("items");
		Map itemsMap = (Map) itemsList.get(0);
		return getTextElementType(itemsMap);
	}

	private String getObjectTextElementType(Map jsonSchemaMap, String elementName) {
		Map objectsMap = (Map) jsonSchemaMap.get(elementName);
		return getTextElementType(objectsMap);
	}

	private String getTextElementType(Map objectsMap) {
		if (!(objectsMap.containsKey("value"))) {
			return "null";
		}
		String output = (String) ((Map) (Map) objectsMap.get("value")).get("type");
		return output;
	}

	private String getAttributeFieldName(String qName, final String uri) {

		final String[] qNameOriginalArray = qName.split(":");
		qName = this.getNamespacesAndIdentifiersAddedFieldName(uri, qNameOriginalArray[qNameOriginalArray.length - 1],
				null);
		final String[] qNameArray = qName.split(":");
		if (qNameArray.length > 1) {
			return "attr_" + qNameArray[0] + ":" + qNameArray[qNameArray.length - 1];

		}
		return "attr_" + qName;

	}

	private String getNamespacesAndIdentifiersAddedFieldName(final String uri, final String localName,
			final OMElement omElement) {
		String modifiedLocalName = null;

		OMNamespace xsiNamespace = null;
		final String prefix = this.getInputSchema().getPrefixForNamespace(uri);
		if (StringUtils.isNotEmpty(prefix)) {
			modifiedLocalName = prefix + ":" + localName;
		} else {
			modifiedLocalName = localName;
		}
		final String prefixInMap = this.inputSchema.getNamespaceMap().get("http://www.w3.org/2001/XMLSchema-instance");
		if (prefixInMap != null && omElement != null) {
			final String xsiType = omElement
					.getAttributeValue(new QName("http://www.w3.org/2001/XMLSchema-instance", "type", prefixInMap));
			if (xsiType != null) {
				final String[] xsiNamespacePrefix = xsiType.split(":", 2);
				xsiNamespace = omElement.findNamespaceURI(xsiNamespacePrefix[0]);
				if (xsiNamespace != null) {
					final String namespaceURI = xsiNamespace.getNamespaceURI();
					modifiedLocalName = modifiedLocalName + "," + prefixInMap + ":type="
							+ this.getInputSchema().getPrefixForNamespace(namespaceURI) + ":" + xsiNamespacePrefix[1];

				} else {
					modifiedLocalName = modifiedLocalName + "," + prefixInMap + ":type=" + xsiType;
				}
			}
		}
		return modifiedLocalName;
	}

	private boolean isXsiNil(final OMElement omElement) {
		final String prefixInMap = this.inputSchema.getNamespaceMap().get("http://www.w3.org/2001/XMLSchema-instance");
		if (prefixInMap != null && omElement != null) {
			final String xsiNilValue = omElement
					.getAttributeValue(new QName("http://www.w3.org/2001/XMLSchema-instance", "nil", prefixInMap));
			if (xsiNilValue != null && "true".equalsIgnoreCase(xsiNilValue)) {
				return true;
			}
		}
		return false;
	}

	public String getAttributeQName(final OMNamespace omNamespace, final String localName) {
		if (omNamespace != null) {
			return omNamespace.getPrefix() + ":" + localName;
		}
		return localName;

	}

	public Schema getInputSchema() {
		return this.inputSchema;

	}

	private void writeFieldElement(final String fieldName, final String valueString, final String fieldType)
			throws IOException, JSException, SchemaException, ReaderException {

		switch (fieldType) {
		case "string": {
			this.jsonBuilder.writeField(this.getModifiedFieldName(fieldName), (Object) valueString, fieldType);
			break;
		}
		case "boolean": {
			this.jsonBuilder.writeField(this.getModifiedFieldName(fieldName),
					(Object) Boolean.parseBoolean(valueString), fieldType);
			break;
		}
		case "number": {
			this.jsonBuilder.writeField(this.getModifiedFieldName(fieldName), (Object) Double.parseDouble(valueString),
					fieldType);
			break;
		}
		case "integer": {
			this.jsonBuilder.writeField(this.getModifiedFieldName(fieldName), (Object) Integer.parseInt(valueString),
					fieldType);
			break;
		}
		default: {
			this.jsonBuilder.writeField(this.getModifiedFieldName(fieldName), (Object) valueString, fieldType);
			break;
		}
		}
	}

	private void writePrimitiveElement(final String valueString, final String fieldType)
			throws IOException, JSException, SchemaException, ReaderException {
		switch (fieldType) {
		case "string": {
			this.jsonBuilder.writePrimitive((Object) valueString, fieldType);
			break;
		}
		case "boolean": {
			this.jsonBuilder.writePrimitive((Object) Boolean.parseBoolean(valueString), fieldType);
			break;
		}
		case "number": {
			this.jsonBuilder.writePrimitive((Object) Double.parseDouble(valueString), fieldType);
			break;
		}
		case "integer": {
			this.jsonBuilder.writePrimitive((Object) Integer.parseInt(valueString), fieldType);
			break;
		}
		default: {
			this.jsonBuilder.writePrimitive((Object) valueString, fieldType);
			break;
		}
		}
	}

	private void writeObjectStartElement(final String fieldName)
			throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.writeObjectFieldStart(this.getModifiedFieldName(fieldName));
	}

	private void writeObjectEndElement() throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.writeEndObject();

	}

	private void writeArrayStartElement(final String fieldName)
			throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.writeArrayFieldStart(this.getModifiedFieldName(fieldName));
	}

	private void writeArrayEndElement() throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.writeEndArray();
	}

	private void writeTerminateElement() throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.close();
		final String jsonBuiltMessage = this.jsonBuilder.getContent();
		this.messageBuilder.notifyWithResult(jsonBuiltMessage);
	}

	private void writeAnonymousObjectStartElement() throws IOException, JSException, SchemaException, ReaderException {
		this.jsonBuilder.writeStartObject();
	}

	private String getModifiedFieldName(final String fieldName) {
		return fieldName.replace(":", "_").replace(",", "_").replace("=", "_");
	}

}