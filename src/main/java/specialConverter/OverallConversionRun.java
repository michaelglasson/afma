package specialConverter;

import java.io.InputStream;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobOutputStream;

/*
 * The aim is to convert an original AFMA input file into a compressed Avro file.
 * Intermediate steps are to:
 * 
 * 1. Extract the schema header from the original file and translate this into
 *    Avro schema language. The schema defines a jsonlines file that has one
 *    object per line of input, with no top-level object. It's not valid json.
 *    instead, it's valid jsonlines.
 *    
 * 2. Convert the body of the original file into jsonlines in a new file. The
 *    conversion deals with the use of empty strings even for numeric values,
 *    creating nulls in their stead. It supports the Avro decoder by wrapping
 *    each data item in a type declaration like {"catch_kg": "int": 50}. It
 *    strips the header and the trailing object and array close symbols. It
 *    takes about three minutes to run on a desktop PC on the largest AFMA
 *    file of 8GB.
 *    
 * 3. Convert the jsonlines file into Avro using the Apache Avro library. The
 *    conversion includes the use of the deflate compression algorithm. It takes
 *    only a couple of minutes to process the largest AFMA file and compresses
 *    the output to a small fraction of the original size. The 8GB AFMA file 
 *    becomes a 143MB Avro file.
 *    
 * The schema translation in this version (210930) uses the double Avro type 
 * as the default for number fields in the original. This allows it to handle 
 * both integral and fractional types. The field name is also used as a hint that
 * about what type is required. Field names such as 'number_of...' or 'no_of...'
 * can probably be relied on to infer a need for an integral type, but at this
 * time, only one field has been given this special treatment.
 * 
 * Given the importance of 'record_no' as a potential record key, this item is
 * coded as an integral type anyway. Schema mappings are in the Avro-
 * SchemaFromAFMAjson class.
 * 
 * Usage at the moment involves editing the properties file and running the main
 * function.
 *    
 */

public class OverallConversionRun extends Thread {
	static final Logger logger = LoggerFactory.getLogger(OverallConversionRun.class);
	static final Map<String, LocalTime> times = new HashMap<>();
	String connectionString = "";
	String blobPrefix = "";
	String blobContainer = "";

	public OverallConversionRun(String connectionString, String blobPrefix, String blobContainer) {
		this.connectionString = connectionString;
		this.blobPrefix = blobPrefix;
		this.blobContainer = blobContainer;
	}

	public void run() {
		logger.info("Processing Azure storage blobs in container: " + blobContainer + ", with prefix: " + blobPrefix);
		MonitorRunServlet.addUpdate(
				"Processing Azure storage blobs in container: " + blobContainer + ", with prefix: " + blobPrefix);

		String avroExtension = ".avro";

		try {
			BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString)
					.buildClient();

			BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(blobContainer);
			ListBlobsOptions options = new ListBlobsOptions();
			options.setPrefix(blobPrefix);

			Set<String> inputBlobs = new LinkedHashSet<>();

			for (BlobItem blobItem : containerClient.listBlobs(options, null)) {
				if (blobItem.getName().endsWith(".json-formatted")) {
					inputBlobs.add(blobItem.getName());
				}
			}

			timerStart("overall run");
			logger.info("Total number of files to process: " + inputBlobs.size());
			MonitorRunServlet.addUpdate("Total number of files to process: " + inputBlobs.size());

			Integer goodFileCount = 0;
			Integer badFileCount = 0;

			for (String itemToProcess : inputBlobs) {
				if (CancelRunServlet.cancelIsPending()) {
					logger.info("Run cancelled by user.");
					MonitorRunServlet.addUpdate("Run cancelled by user at "
							+ LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
					CancelRunServlet.resetPendingCancel();
					StartRunServlet.runIsInProgress = false;
					return;
				}

				BlobClient blobInputClient = containerClient.getBlobClient(itemToProcess);
				InputStream blobIS = blobInputClient.openInputStream();

				BlobClient blobOutputClient = containerClient.getBlobClient(itemToProcess + avroExtension);
				BlobOutputStream blobOS = blobOutputClient.getBlockBlobClient().getBlobOutputStream(true);

				// Now we invoke the conversion
				MonitorRunServlet.addUpdate("Processing file: " + itemToProcess + " starting at "
						+ LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());

				logger.info("Processing file: " + itemToProcess + " starting at "
						+ LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
				// timerStart("Converting AFMA json file to Avro");
				if (ConvertOneAFMAFiletoAvro.run(blobIS, blobOS))
					goodFileCount++;
				else
					badFileCount++;
				// timerStop("Converting AFMA json file to Avro");
			}

			timerStop("overall run");
			MonitorRunServlet.addUpdate("The number of files successfully processed is: " + goodFileCount.toString());
			MonitorRunServlet.addUpdate("The number of files UNsuccessfully processed is: " + badFileCount.toString());
			logger.info(
					"Run completed at " + LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
			MonitorRunServlet.addUpdate(
					"Run completed at " + LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
			StartRunServlet.runIsInProgress = false;
		} catch (Exception e) {
			logger.error("ERROR: blobs not found: " + e.getMessage());
			MonitorRunServlet
					.addUpdate("ERROR: blobs not found (check connection string and container): " + e.getMessage());
			MonitorRunServlet.addUpdate("Run ended with error at "
					+ LocalTime.now(ZoneId.of("Australia/Canberra")).format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
			StartRunServlet.runIsInProgress = false;
			return;
		}
	}

	public void timerStart(String data) {
		times.put(data, LocalTime.now(ZoneId.of("Australia/Canberra")));
		logger.info(data + " started at " + times.get(data.toString()));
	}

	public void timerStop(String data) {
		Duration duration = Duration.between(times.get(data), LocalTime.now(ZoneId.of("Australia/Canberra")));
		String minutes = String.valueOf(duration.toMinutes());
		String seconds = String.valueOf(duration.getSeconds() % 60);
		logger.info("Elapsed time for " + data + " was: " + minutes + " minutes and " + seconds + " seconds");
		MonitorRunServlet
				.addUpdate("Elapsed time for " + data + " was: " + minutes + " minutes and " + seconds + " seconds");
	}
}
