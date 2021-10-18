package afmafile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;

/*
 * Extract the schema from the top of an AFMA data json file and create an Avro
 * Schema object. The schema can later be used to generate an Avro
 * file fromJsonLines.
 */

public class AvroSchemaFromAFMASchemaStringNoThrows {
	public static ByteArrayOutputStream schemaStream = new ByteArrayOutputStream();
	public static JsonGenerator jGenerator;
	public static Map<String, fieldType> finalTypeMap = new HashMap<>();
	public static Schema schema;

	public static Boolean makeSchema(String schemaString) {
		schemaStream.reset();
		finalTypeMap.clear();
		try (JsonParser jParser = new JsonFactory().createParser(schemaString)) {
			schemaStream.reset();
			jGenerator = new JsonFactory().createGenerator(schemaStream);
			writeSchemaHeaderAndFooterToSchemaStream();
			// Retrieve the 'columns' schema from AFMA raw JSON.
			while (jParser.getCurrentName() != "columns")
				jParser.nextToken();
			jParser.nextToken();
			String nameString = "";
			String typeString = "";
			while (jParser.nextToken() != JsonToken.END_ARRAY) {
				if ("name".equals(jParser.getCurrentName())) {
					jParser.nextToken();
					nameString = jParser.getText().toLowerCase();
					jParser.nextToken();
					if ("type".equals(jParser.getCurrentName())) {
						jParser.nextToken();
						typeString = jParser.getText();
					}
					writeFieldDefinitionsToOut(nameString, typeString);
				}
			}
			jGenerator.close();
			writeSchema();
			return true;
		} catch (IOException e) {
			MonitorRunServlet.addUpdate("Something wrong with input file schema" + e.getMessage());
			return false;
		} catch (Exception e) {
			MonitorRunServlet.addUpdate("Something wrong with input file schema" + e.getMessage());
			return false;
		}
	}

	public static void writeFieldDefinitionsToOut(String name, String type) throws Exception {
			jGenerator.writeStartObject();
			jGenerator.writeStringField("name", name);
			jGenerator.writeFieldName("type");

			jGenerator.writeStartArray();

			jGenerator.writeString("null");

			// If the type is a date, then we need to do extra

			writeAvroType(name, type);

			jGenerator.writeEndArray();

			jGenerator.writeStringField("default", null);

			jGenerator.writeEndObject();
	}

	public static void writeSchemaHeaderAndFooterToSchemaStream() throws Exception {
			jGenerator.useDefaultPrettyPrinter();
			jGenerator.writeStartObject();
			jGenerator.writeStringField("namespace", "awe.gov.au");
			jGenerator.writeStringField("name", "items");
			jGenerator.writeStringField("type", "record");
			jGenerator.writeFieldName("fields");
			jGenerator.writeStartArray();
	}

	/*
	 * This allows us to use either the AFMA schema type or the AFMA schema field
	 * name to determine the outgoing Avro type. For a start, we will just convert
	 * record_no to an int, but in future, we might try different approaches with
	 * field names that contain thing likek 'number_of'.
	 */
	public static void writeAvroType(String name, String AFMAType) throws Exception {
		Map<String, fieldType> typeMap = new HashMap<>();
		typeMap.put("CHAR", new fieldType("string", ""));
		typeMap.put("VARCHAR2", new fieldType("string", ""));
		typeMap.put("NUMBER", new fieldType("double", ""));
		typeMap.put("DATE", new fieldType("long", "timestamp-millis"));

		Map<String, fieldType> nameMap = new HashMap<>();

		nameMap.put("record_no", new fieldType("int", ""));
		String physicalType = "";
		String logicalType = "";

		if (nameMap.get(name) != null)
			physicalType = nameMap.get(name).physicalType;

		else if (typeMap.get(AFMAType) != null) {
			physicalType = typeMap.get(AFMAType).physicalType;
			logicalType = typeMap.get(AFMAType).logicalType;
		}

		else
			physicalType = "string";

		if (AFMAType.equals("DATE")) {

			jGenerator.writeStartObject();
			jGenerator.writeStringField("type", "long");
			jGenerator.writeStringField("logicalType", "timestamp-millis");
			jGenerator.writeEndObject();

		} else {
			jGenerator.writeString(physicalType);
		}
		finalTypeMap.put("\"" + name + "\"", new fieldType("\"" + physicalType + "\"", "\"" + logicalType + "\""));
	}

	public static void writeSchema() {
		try {
			schema = new Schema.Parser().parse(schemaStream.toString("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			MonitorRunServlet.addUpdate("UnsupportedEncodingException writing schema");
		}
	}

	public static class fieldType {
		fieldType(String physicalType, String logicalType) {
			this.physicalType = physicalType;
			this.logicalType = logicalType;
		}

		public String physicalType;
		public String logicalType;
	}

}
