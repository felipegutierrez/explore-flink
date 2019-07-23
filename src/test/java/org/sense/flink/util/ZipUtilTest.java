package org.sense.flink.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.FileUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ZipUtilTest extends TestCase {
	public ZipUtilTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(ZipUtilTest.class);
	}

	public void testUnpackZipFileFromValenciaOpenDataPortal() {
		try {
			FileUtils.deleteDirectory(new File("out/zip/noise"));

			URL url = new URL("http://mapas.valencia.es/lanzadera/opendata/mapa_ruido/SHAPE");
			File destDir = new File("out/zip/noise");
			ZipUtil.unpackZipFile(url, destDir);

			File dbf = new File("out/zip/noise/MAPA_RUIDO.dbf");
			File prj = new File("out/zip/noise/MAPA_RUIDO.prj");
			File shp = new File("out/zip/noise/MAPA_RUIDO.shp");
			File shx = new File("out/zip/noise/MAPA_RUIDO.shx");

			assertTrue(dbf.exists());
			assertTrue(prj.exists());
			assertTrue(shp.exists());
			assertTrue(shx.exists());
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
