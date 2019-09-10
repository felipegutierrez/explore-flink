package org.sense.flink.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.opengis.referencing.operation.TransformException;
import org.sense.flink.pojo.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleGeographicalPolygons implements Serializable {
	private static final long serialVersionUID = 7331398841196874219L;
	private static final Logger logger = LoggerFactory.getLogger(SimpleGeographicalPolygons.class);
	private static final String DEFAULT_VALENCIA_DISTRICTS_POLYGONS = "/valencia/admin_level_9_Valencia_polygons.geojson";

	private File geoJSON = File.createTempFile("temp", ".geojson");
	private DataStore dataStore;
	private FilterFactory2 filerFactory2;
	private SimpleFeatureSource simpleFeatureSource;
	private CRSCoordinateTransformer crsCoordinateTransformer;

	/**
	 * The Valencia GeoJSON file comes on the format urn:ogc:def:crs:OGC:1.3:CRS84,
	 * which is equivalent to the format EPSG:4326 on GeoTools
	 * 
	 * @throws Exception
	 */
	public SimpleGeographicalPolygons() throws Exception {
		InputStream in = getClass().getResourceAsStream(DEFAULT_VALENCIA_DISTRICTS_POLYGONS);
		if (in == null) {
			throw new Exception("GeoJSON file which contains the polygons does not exist!");
		}
		FileUtils.copyInputStreamToFile(in, geoJSON);
		if (!geoJSON.exists()) {
			throw new Exception("GeoJSON file [" + geoJSON + "] which contains the polygons does not exist!");
		}
		this.createDataStore();
		this.crsCoordinateTransformer = new CRSCoordinateTransformer();
	}

	private void createDataStore() {
		try {
			if (!geoJSON.exists()) {
				throw new Exception("GeoJSON file does not exits!");
			}
			Map<String, Object> params = new HashMap<String, Object>();
			params.put(GeoJSONDataStoreFactory.URLP.key, URLs.fileToUrl(geoJSON));
			dataStore = DataStoreFinder.getDataStore(params);
			filerFactory2 = CommonFactoryFinder.getFilterFactory2();
			simpleFeatureSource = dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Long getOsmId(Point point) {
		try {
			Point pointLonLat = null;
			if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)) {
				pointLonLat = point;
			} else if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)) {
				pointLonLat = crsCoordinateTransformer.xyToLonLatPoint(point.getX(), point.getY());
			} else {
				return null;
			}
			Contains contains = filerFactory2.contains(
					filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
					filerFactory2.literal(pointLonLat.toString()));
			SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
			if (collection.size() > 0) {
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						SimpleFeature simpleFeature = itr.next();
						return Long.valueOf(simpleFeature.getAttribute("osm_id").toString());
					}
				}
			}
		} catch (IOException | TransformException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Point getRadians(Point point) {
		Point pointLonLat = null;
		try {
			if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)) {
				pointLonLat = crsCoordinateTransformer.xyToRadiusPoint(point.getX(), point.getY());
			} else if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)) {
				pointLonLat = crsCoordinateTransformer.lonLatToRadiusPoint(point.getX(), point.getY());
			} else {
				logger.error("CRS Coordinate Reference System not defined!");
			}
		} catch (TransformException e) {
			e.printStackTrace();
		}
		return pointLonLat;
	}

	public Point calculateDerivedPosition(Point point, double range, double bearing) {
		double EarthRadius = 6371000; // m

		Point radians = getRadians(point);

		double latA = radians.getX();
		double lonA = radians.getY();
		double angularDistance = range / EarthRadius;
		double trueCourse = Math.toRadians(bearing);

		double lat = Math.asin(Math.sin(latA) * Math.cos(angularDistance)
				+ Math.cos(latA) * Math.sin(angularDistance) * Math.cos(trueCourse));

		double dlon = Math.atan2(Math.sin(trueCourse) * Math.sin(angularDistance) * Math.cos(latA),
				Math.cos(angularDistance) - Math.sin(latA) * Math.sin(lat));

		double lon = ((lonA + dlon + Math.PI) % (Math.PI * 2)) - Math.PI;

		lat = Math.toDegrees(lat);
		lon = Math.toDegrees(lon);

		Point newPoint = new Point(lat, lon);

		return newPoint;

	}

	/**
	 * This method returns a Tuple3 object containing the districtID, administration
	 * level, and the district name.
	 * 
	 * @param point
	 * @return Tuple3<districtId, adminLevel, districtName>
	 */
	public Tuple3<Long, Long, String> getAdminLevel(Point point) {
		try {
			Point pointLonLat = null;
			if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)) {
				pointLonLat = point;
			} else if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)) {
				pointLonLat = crsCoordinateTransformer.xyToLonLatPoint(point.getX(), point.getY());
			} else {
				logger.error("CRS Coordinate Reference System not defined!");
				return Tuple3.of(null, null, "");
			}
			Contains contains = filerFactory2.contains(
					filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
					filerFactory2.literal(pointLonLat.toString()));
			SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
			if (collection.size() > 0) {
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						SimpleFeature simpleFeature = itr.next();
						String districtName = simpleFeature.getAttribute("name").toString();
						Long adminLevel = Long.valueOf(simpleFeature.getAttribute("admin_level").toString());
						Long districtId = Long.valueOf(ValenciaDistricts.getDistrictId(districtName));
						return Tuple3.of(districtId, adminLevel, districtName);
					}
				}
			}
		} catch (IOException | TransformException e) {
			e.printStackTrace();
		}
		return Tuple3.of(null, null, "");
	}

	public Long getAdminLevelId(Point point) {
		try {
			Point pointLonLat = null;
			if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)) {
				pointLonLat = point;
			} else if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)) {
				pointLonLat = crsCoordinateTransformer.xyToLonLatPoint(point.getX(), point.getY());
			} else {
				return null;
			}
			Contains contains = filerFactory2.contains(
					filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
					filerFactory2.literal(pointLonLat.toString()));
			SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
			if (collection.size() > 0) {
				try (SimpleFeatureIterator itr = collection.features()) {
					while (itr.hasNext()) {
						SimpleFeature simpleFeature = itr.next();
						String adminLevelName = simpleFeature.getAttribute("name").toString();
						return Long.valueOf(ValenciaDistricts.getDistrictId(adminLevelName));
					}
				}
			}
		} catch (IOException | TransformException e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean contains(Point point) {
		if (getOsmId(point) != null) {
			return true;
		}
		return false;
	}

	public void printProperties(List<Point> points) {
		try {
			for (Point point : points) {
				Point pointLonLat = null;
				if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)) {
					pointLonLat = point;
				} else if (point.getCsr().equals(CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)) {
					pointLonLat = crsCoordinateTransformer.xyToLonLatPoint(point.getX(), point.getY());
				} else {
					return;
				}
				Contains contains = filerFactory2.contains(
						filerFactory2.property(simpleFeatureSource.getSchema().getGeometryDescriptor().getLocalName()),
						filerFactory2.literal(pointLonLat.toString()));
				SimpleFeatureCollection collection = simpleFeatureSource.getFeatures(contains);
				if (collection.size() > 0) {
					System.out.println("Found point [" + pointLonLat.toString() + "] on the file ["
							+ geoJSON.getAbsolutePath() + "]");
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
			}
		} catch (IOException | TransformException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		// @formatter:off
		SimpleGeographicalPolygons sgp = new SimpleGeographicalPolygons();
		List<Point> points = new ArrayList<Point>();
		// points.add(new Point(-0.3774, 39.4698, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)); // 1,9,Ciutat Vella
		// points.add(new Point(-0.3630, 39.4477, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326)); // 10,9,Quatre Carreres

		// points.add(new Point(725685.2117, 4372281.7883, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 1,9,Ciutat Vella
		// points.add(new Point(725704.3880000003, 4370895.1142, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 2,9,l'Eixample
		// points.add(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 3,9,Extramurs
		// points.add(new Point(722864.4094000002, 4373373.1132, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 4,9,Campanar
		// points.add(new Point(726236.4035999998, 4373308.101, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 5,9,la Saïdia
		// points.add(new Point(726423.431815, 4373034.130474, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 6,9,Pla del Real
		// points.add(new Point(723969.784325, 4372420.897437, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 7,9,Olivereta
		// points.add(new Point(723623.655515, 4370778.759536, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 8,9,Patraix
		// points.add(new Point(724034.3761, 4369995.1312, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 9,9,Jesús
		// points.add(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 10,9,Quatre Carreres
		// points.add(new Point(730007.679276, 4369416.885897, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 11,9,Poblats Maritims
		// points.add(new Point(728630.3849999998, 4370921.0409, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 12,9,Camins al Grau
		// points.add(new Point(729010.3865999999, 4373390.0215, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 13,9,Algirós
		// points.add(new Point(727338.396370, 4374107.624796, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 14,9,Benimaclet 
		// points.add(new Point(726240.844004, 4375087.354217, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 15,9,Rascanya
		points.add(new Point(724328.279007, 4374887.874634, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830)); // 16,9,Benicalap

		sgp.printProperties(points);
		// @formatter:on
	}
}
