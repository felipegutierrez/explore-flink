package org.sense.flink.source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.sense.flink.pojo.ValenciaNoise;
import org.sense.flink.util.ZipUtil;

/**
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaNoiseConsumer extends RichSourceFunction<ValenciaNoise> {
	private static final long serialVersionUID = -2373602610753429092L;
	private static final String VALENCIA_NOISE_URL = "http://mapas.valencia.es/lanzadera/opendata/mapa_ruido/SHAPE";
	private static final String OUTPUT_DIR = "out/noise/zip";
	private String urlZipFile;
	private long delayTime;

	public ValenciaNoiseConsumer() {
		this(VALENCIA_NOISE_URL, 10000);
	}

	public ValenciaNoiseConsumer(String urlZipFile) {
		this(urlZipFile, 10000);
	}

	public ValenciaNoiseConsumer(String urlZipFile, long delayTime) {
		this.urlZipFile = urlZipFile;
		this.delayTime = delayTime;
	}

	@Override
	public void run(SourceContext<ValenciaNoise> ctx) throws Exception {

		URL url = new URL(this.urlZipFile);

		while (true) {
			ZipUtil.unpackZipFile(url, new File(OUTPUT_DIR));

			// TODO
			ctx.collect(null);

			Thread.sleep(delayTime);
		}
	}

	@Override
	public void cancel() {
	}

	public static void main(String[] args) {
		try {
			// reading shapefile
			File file = new File("out/noise/zip/MAPA_RUIDO.shp");
			FileDataStore myData = FileDataStoreFinder.getDataStore(file);
			SimpleFeatureSource source = myData.getFeatureSource();
			SimpleFeatureType schema = source.getSchema();

			Query query = new Query(schema.getTypeName());
			query.setMaxFeatures(1);

			FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(query);
			try (FeatureIterator<SimpleFeature> features = collection.features()) {
				while (features.hasNext()) {
					SimpleFeature feature = features.next();
					System.out.println(feature.getID() + ": ");
					for (Property attribute : feature.getProperties()) {
						if ("the_geom".equals(attribute.getName().toString())) {
							System.out.println("\t" + attribute.getName() + ": "
									+ attribute.getValue().toString().substring(0, 100));
						}
					}
				}
			}

			// reading DBF file
			FileInputStream fis = new FileInputStream("out/noise/zip/MAPA_RUIDO.dbf");
			DbaseFileReader dbfReader = new DbaseFileReader(fis.getChannel(), false, Charset.forName("ISO-8859-1"));

			while (dbfReader.hasNext()) {
				final Object[] fields = dbfReader.readEntry();

				Object field1 = fields[0];
				Object field2 = fields[1];

				System.out.println("DBF field 1 value is: " + field1 + " - DBF field 2 value is: " + field2);
			}

			dbfReader.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
