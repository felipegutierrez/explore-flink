package org.sense.flink.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.geojson.GeoJSONDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.util.URLs;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.Contains;
import org.sense.flink.pojo.Point;

public class GeoJSONUtil {

	public static boolean isInside(Point point, File geoJSON) {
		if (Strings.isNullOrEmpty(getOsmId(point, geoJSON))) {
			return false;
		}
		return true;
	}

	public static String getOsmId(Point point, File geoJSON) {
		try {
			String p = point.toString();
			if (!geoJSON.exists()) {
				return "";
			}
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(GeoJSONDataStoreFactory.URLP.key, URLs.fileToUrl(geoJSON));
			DataStore newDataStore = DataStoreFinder.getDataStore(params);

			FilterFactory2 filerFactory2 = CommonFactoryFinder.getFilterFactory2();
			SimpleFeatureSource simpleFeatureSource = newDataStore.getFeatureSource(newDataStore.getTypeNames()[0]);
			Contains contains = filerFactory2.contains(
					filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
					filerFactory2.literal(p));
			SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
			if (collection.size() > 0) {
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						SimpleFeature simpleFeature = itr.next();
						return simpleFeature.getAttribute("osm_id").toString();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	public static void main(String[] args) {
		try {
			String p = "Point(-0.3630 39.4477)";
			File inFile = new File("resources/valencia/admin_level_9_Valencia_polygons.geojson");
			if (!inFile.exists()) {
				return;
			}
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(GeoJSONDataStoreFactory.URLP.key, URLs.fileToUrl(inFile));
			DataStore newDataStore = DataStoreFinder.getDataStore(params);

			FilterFactory2 filerFactory2 = CommonFactoryFinder.getFilterFactory2();
			SimpleFeatureSource simpleFeatureSource = newDataStore.getFeatureSource(newDataStore.getTypeNames()[0]);
			Contains contains = filerFactory2.contains(
					filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
					filerFactory2.literal(p));
			SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
			if (collection.size() > 0) {
				System.out.println("Found point [" + p + "] on the file [" + inFile.getAbsolutePath() + "]");
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						SimpleFeature simpleFeature = itr.next();
						Iterator<Property> properties = simpleFeature.getProperties().iterator();
						while (properties.hasNext()) {
							Property property = properties.next();
							System.out.println(
									property.getName() + " : " + simpleFeature.getAttribute(property.getName()));
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
