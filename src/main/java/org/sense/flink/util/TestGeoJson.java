package org.sense.flink.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.geojson.GeoJSONDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.util.URLs;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.Contains;

public class TestGeoJson {
	public static void main(String[] args) {
		try {
			File inFile = new File("resources/valencia/boundary_administrative_Valencia_lines.geojson");
			if (!inFile.exists()) {
				return;
			}
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(GeoJSONDataStoreFactory.URLP.key, URLs.fileToUrl(inFile));
			DataStore newDataStore = DataStoreFinder.getDataStore(params);
			
			String pt = "Point (0.1778961 38.8127748)";

			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			SimpleFeatureSource featureSource = newDataStore.getFeatureSource(newDataStore.getTypeNames()[0]);
			Contains contains = ff.contains(
					ff.property(featureSource.getSchema().getGeometryDescriptor().getLocalName()), ff.literal(pt));
			SimpleFeatureCollection collection = featureSource.getFeatures(contains);
			if (collection.size() > 0) {
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						System.out.println(itr.next());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
