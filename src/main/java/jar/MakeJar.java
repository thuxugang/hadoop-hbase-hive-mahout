package jar;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 原来完整的详见EJob
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class MakeJar {

	/**
	 * Create a temp jar file in "java.io.tmpdir".
	 * 
	 * @param root
	 * @return
	 * @throws IOException
	 */
	public static File createTempJar(String root) throws IOException {
		if (!new File(root).exists()) {
			return null;
		}
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
		final File jarFile = File.createTempFile("jar.MakeJar-", ".jar", new File(System
				.getProperty("java.io.tmpdir")));

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				jarFile.delete();
			}
		});

		JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile),
				manifest);
		createTempJarInner(out, new File(root), "");
		out.flush();
		out.close();
		return jarFile;
	}

	private static void createTempJarInner(JarOutputStream out, File f,
			String base) throws IOException {
		if (f.isDirectory()) {
			File[] fl = f.listFiles();
			if (base.length() > 0) {
				base = base + "/";
			}
			for (int i = 0; i < fl.length; i++) {
				createTempJarInner(out, fl[i], base + fl[i].getName());
			}
		} else {
			out.putNextEntry(new JarEntry(base));
			FileInputStream in = new FileInputStream(f);
			byte[] buffer = new byte[1024];
			int n = in.read(buffer);
			while (n != -1) {
				out.write(buffer, 0, n);
				n = in.read(buffer);
			}
			in.close();
		}
	}

}
