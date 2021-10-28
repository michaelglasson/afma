package specialConverter;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

/*
 * Convert original AFMA json-formatted file into an avro file. Read the file line by line, skipping 
 * the schema definition at the top of the file. When you encounter a '{', staring building up the 
 * json line one field at a time. Ouput the whole line when you encounter a '}'. Make sure you handle
 * commas at the end of items. Use the finalTypeMap lookup to determine the object type for fields 
 * for Avro compatibility. 
 */

public class ConvertOneAFMAFiletoAvro {

	public static Boolean run(InputStream inputFile, OutputStream avroFile) {
		try {
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(inputFile));
			String line;
			StringBuilder sb = new StringBuilder();
			StringBuilder schemaStringBuilder = new StringBuilder();
			String beforeColon = "";
			String afterColon = "";
			Object datum = "";
			DatumReader<Object> datumReader = null;
			JsonDecoder decoder = null;
			DataFileWriter<Object> datumWriter = new DataFileWriter<>(new GenericDatumWriter<>());
			datumWriter.setCodec(CodecFactory.deflateCodec(5));

			// 9999-12-31 23:59:59
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
			ZoneOffset zoneOffset = ZoneOffset.ofHours(10); // assume time zone of Australian Eastern Standard Time
			Boolean beforeStart = true;

			while ((line = inputReader.readLine()) != null) {
				if (beforeStart) {
					if (line.contains("\"items\"")) {
						beforeStart = false;
						if (!AvroSchemaFromAFMASchemaString.makeSchema(schemaStringBuilder.toString())) return false;
						datumReader = new GenericDatumReader<>(AvroSchemaFromAFMASchemaString.schema);
						decoder = DecoderFactory.get().jsonDecoder(AvroSchemaFromAFMASchemaString.schema, "");
						datumWriter.create(AvroSchemaFromAFMASchemaString.schema, avroFile);

					} else {
						schemaStringBuilder.append(line);
					}
					continue;
				}
				if (line.contains("]"))
					break;

				sb.append("{");
				while (!(line = (inputReader.readLine()).trim()).startsWith("}")) {
					if (line.startsWith("{"))
						continue;
					int i = line.indexOf(":");
					beforeColon = line.substring(0, i).trim();
					sb.append(beforeColon + ":");
					afterColon = line.substring(i + 1, line.length()).trim();
					if (afterColon.charAt(afterColon.length() - 1) == ',')
						afterColon = afterColon.substring(0, afterColon.length() - 1);
					afterColon = afterColon.replaceAll(" *\"", "\"");

					// Empty strings and invalid dates become nulls
					if (afterColon.contains("\"\"") || afterColon.startsWith("\"0001")
							|| afterColon.startsWith("\"9999")) {
						sb.append("null,");
						continue;
					}
					// Convert date types to epoch time (seconds before or since 1 Jan 1970
					if (AvroSchemaFromAFMASchemaString.finalTypeMap.get(beforeColon).logicalType.equals("\"timestamp-millis\"")) {
						afterColon = afterColon.replaceAll("\"", "");
						LocalDateTime ldt = LocalDateTime.parse(afterColon, dtf);
						long timestamp = ldt.toEpochSecond(zoneOffset);
						afterColon = String.valueOf(timestamp);
					}
					sb.append("{" + AvroSchemaFromAFMASchemaString.finalTypeMap.get(beforeColon).physicalType + ":" + afterColon
							+ "},");
				}
				if (sb.charAt(sb.length() - 1) == ',')
					sb.deleteCharAt(sb.length() - 1);
				sb.append("}\n");

				decoder.configure(sb.toString());

				datum = datumReader.read(null, decoder);
				datumWriter.append(datum);

				sb.setLength(0);
			}
			inputReader.close();
			datumWriter.close();
			return true;
		} catch (Exception e) {
			MonitorRunServlet.addUpdate("ERROR: something wrong with the input file: " + e.getMessage());
			return false;
		}
	}
}
