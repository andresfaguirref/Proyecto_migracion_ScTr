package sdm.migrador;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.macroproyectos.ecm.ContainerManager;
import com.macroproyectos.ecm.node.MPDataNode;
import com.macroproyectos.ecm.node.MPNodeType;
import com.macroproyectos.ecm.node.MPProperty;
import com.macroproyectos.ecm.store.DocumentStoreManager;

public class Main {

	static Logger log = LoggerFactory.getLogger(Main.class.getName());

	static Thread[] threads;
	static boolean runnig = true;
	static boolean noMore;

	static Connection conn;
	static PreparedStatement ps;
	static ResultSet rs;
	static PreparedStatement psInsert;
	static PreparedStatement psUpdate;
	static PreparedStatement psProps;
	static PreparedStatement psPath;

	static int migrados;

	static Object typesLock = new Object();

	static MPNodeType carpeta;
	static MPNodeType archivo;
	static Map<String, MPNodeType> nodeTypes;

	static Pattern regex = Pattern.compile("[^a-zA-Z0-9]");

	static String username;
	static String password;
	static String repository;

	static Args params;

	static Map<Character, Character> chars;
	static {
		chars = new HashMap<>();
		chars.put('\u00C1', 'A');
		chars.put('\u00C9', 'E');
		chars.put('\u00CD', 'I');
		chars.put('\u00D3', 'O');
		chars.put('\u00DA', 'U');
		chars.put('\u00E1', 'a');
		chars.put('\u00E9', 'e');
		chars.put('\u00ED', 'i');
		chars.put('\u00F3', 'o');
		chars.put('\u00FA', 'u');
		chars.put('\u00D1', 'N');
		chars.put('\u00F1', 'n');
	}

	static synchronized Toc findNext() throws SQLException {
		if (!runnig || noMore || params.maximumRecords > 0 && migrados > params.maximumRecords) {
			return null;
		}
		if (rs.next()) {
			return next();
		}
		rs.close();
		rs = ps.executeQuery();
		if (rs.next()) {
			return next();
		}
		noMore = true;
		return null;
	}

	static Toc next() throws SQLException {
		Toc toc = new Toc();
		toc.id = rs.getLong(1);
		toc.name = rs.getString(2);
		toc.path = rs.getString(3);
		toc.type = rs.getInt(4);

		if (toc.isFile()) {
			psPath.setLong(1, toc.id);
			try (ResultSet rs = psPath.executeQuery()) {
				rs.next();
				if (params.dummyPDF == null) {
					toc.file = new File(rs.getString(2), rs.getString(1));
				} else {
					toc.file = params.dummyPDF;
				}
				toc.nodeType = rs.getString(3);
			}

			toc.properties = new HashMap<>();

			psProps.setLong(1, toc.id);
			try (ResultSet rs = psProps.executeQuery()) {
				while (rs.next()) {
					Object value;
					if ("S".equals(rs.getString(2))) {
						value = rs.getString(4);
						if (value == null) {
							value = "";
						}
					} else {
						value = rs.getDate(3);
					}
					toc.properties.put(rs.getString(1), value);
				}
			}
		}

		psInsert.setLong(1, toc.id);
		psInsert.executeUpdate();

		return toc;
	}

	static synchronized void update(long id, String path) throws SQLException {
		psUpdate.setString(1, path);
		psUpdate.setLong(2, id);
		psUpdate.executeUpdate();
		migrados++;
		log.info("Migrados: {}", migrados);
	}

	static void close(AutoCloseable res) {
		if (res != null) {
			try {
				res.close();
			} catch (Exception e) {
				log.error("", e);
			}
		}
	}

	static void stop() {
		log.info("Finalizando migracion");
		runnig = false;
		try {
			for (Thread t : threads) {
				t.join();
			}
		} catch (InterruptedException e) {
			log.error("", e);
			Thread.currentThread().interrupt();
		}
		close(psPath);
		close(psProps);
		close(psUpdate);
		close(psInsert);
		close(rs);
		close(ps);
		close(conn);
		DocumentStoreManager.destroyStores();
		log.info("Total migrados {}", migrados);
		log.info("Migracion finalizada");
	}

	static void addProperty(MPNodeType nodeType, String name, int dataType) {
		MPProperty prop = new MPProperty();
		prop.setName(name);
		prop.setDataType(dataType);
		prop.setAutocreated(false);
		prop.setMandatory(false);
		prop.setMultiple(false);
		nodeType.addProperty(prop);
	}

	static boolean checkNodeTypeProperties(MPNodeType nodeType, Map<String, Object> values, Map<String, Object> props) {
		Map<String, MPProperty> currentProps = nodeType.getProperties();
		boolean modified = false;
		for (Entry<String, Object> property : props.entrySet()) {
			String name = normalize(property.getKey());
			if (!currentProps.containsKey(name)) {
				addProperty(nodeType, name,
						property.getValue() instanceof String ? MPProperty.STRING : MPProperty.DATE);
				modified = true;
			}
			values.put(name, property.getValue());
		}
		return modified;
	}

	static String normalize(String name) {
		Matcher matcher = regex.matcher(name);
		if (matcher.find()) {
			StringBuilder sb = new StringBuilder(name.length());
			int index = 0;
			do {
				int start = matcher.start();
				for (int i = index; i < start; i++) {
					sb.append(name.charAt(i));
				}
				Character c = chars.get(name.charAt(start));
				if (c == null) {
					c = '_';
				}
				sb.append(c);
				index = start + 1;
			} while (matcher.find());
			for (int i = index; i < name.length(); i++) {
				sb.append(name.charAt(i));
			}
			return sb.toString();
		}
		return name;
	}

	static MPNodeType makeNodeType(String name, boolean file) {
		MPNodeType nodeType = new MPNodeType();
		nodeType.setNodeName(name);
		nodeType.setFile(file);
		nodeType.setMixin(false);

		addProperty(nodeType, "tocid", MPProperty.LONG);
		addProperty(nodeType, "nombreOriginal", MPProperty.STRING);

		nodeTypes.put(name, nodeType);
		return nodeType;
	}

	static ContainerManager ecm() {
		ContainerManager cm = new ContainerManager();
		cm.initializeRepository(username, password, repository);
		return cm;
	}

	static Properties props(String path) throws IOException {
		Properties props = new Properties();
		try (InputStream stream = Main.class.getResourceAsStream(path)) {
			props.load(stream);
		}
		return props;
	}

	static void hilo() {
		String threadName = Thread.currentThread().getName();

		log.info("Iniciando hilo {}", threadName);

		ContainerManager cm = ecm();
		try {
			while (runnig) {
				Toc toc = findNext();
				if (toc == null || !runnig) {
					break;
				}

				String name = normalize(toc.name);
				String ruta;
				if ("/".equals(toc.path)) {
					ruta = "/" + name;
				} else {
					ruta = toc.path + "/" + name;
				}

				MPNodeType nodeType;
				Map<String, Object> props = new HashMap<>();
				if (toc.isFile()) {
					log.info("Hilo {}, Archivo {}", threadName, ruta);
					if (toc.nodeType == null) {
						nodeType = archivo;
						synchronized (typesLock) {
							if (checkNodeTypeProperties(nodeType, props, toc.properties)) {
								cm.updateNodeType(nodeType);
							}
						}
					} else {
						String nodeName = normalize(toc.nodeType);
						synchronized (typesLock) {
							nodeType = nodeTypes.get(nodeName);
							if (nodeType == null) {
								nodeType = makeNodeType(nodeName, true);
								checkNodeTypeProperties(nodeType, props, toc.properties);
								cm.loadNodeDataType(nodeType);
							} else {
								if (checkNodeTypeProperties(nodeType, props, toc.properties)) {
									cm.updateNodeType(nodeType);
								}
							}
						}
					}
				} else {
					log.info("Hilo {}, Carpeta {}", threadName, ruta);
					nodeType = carpeta;
				}
				props.put("tocid", toc.id);
				props.put("nombreOriginal", toc.name);

				MPDataNode dataNode = MPDataNode.createDataNode(name, nodeType);
				dataNode.setProperties(props);
				dataNode.setParent(toc.path);

				if (toc.isFile()) {
					dataNode.setContent(Files.readAllBytes(toc.file.toPath()));
					dataNode.setContentFileName(name + ".pdf");
					dataNode.setMimeType("application/pdf");
				}
				try {
					cm.persistDataNode(dataNode);
				} catch (RuntimeException e) {
					throw e;
				}

				update(toc.id, ruta);
			}
		} catch (SQLException | IOException e) {
			throw new MException(e);
		} finally {
			try {
				cm.closeResources();
			} catch (Exception e) {
				log.error("", e);
			}
		}

		log.info("Finalizando hilo {}", threadName);
	}

	static Args parseArgs(String[] args) {
		Args params = new Args();
		if (args == null) {
			return params;
		}
		if (args.length == 1) {
			String arg0 = args[0];
			if ("reset".equals(arg0)) {
				params.reset = true;
				return params;
			}
			if ("help".equals(arg0)) {
				params.help = true;
				return params;
			}
		}
		for (String arg : args) {
			String[] parts = arg.split("=");
			if (parts.length != 2) {
				log.error("Argumento mal formado {}", arg);
				return null;
			}
			String name = parts[0];
			String value = parts[1];
			try {
				switch (name) {
					case "dummy":
						params.dummyPDF = new File(value);
						if (!params.dummyPDF.exists()) {
							log.error("No existe el archivo {}", value);
							return null;
						}
						break;
					case "threads":
						params.totalThreads = Integer.parseInt(value);
						if (params.totalThreads < 1) {
							params.totalThreads = 1;
						}
						break;
					case "max":
						params.maximumRecords = Integer.parseInt(value);
						break;
					default:
						log.error("No se reconoce argumento {}", name);
						return null;
				}
			} catch (NumberFormatException e) {
				log.error("Numero incorrecto {}", value);
				log.debug(e.toString());
				return null;
			}
		}
		return params;
	}

	static void printHelp() {
		log.info("\nArgumentos:\n");
		log.info("help # muestra la ayuda\n");
		log.info("reset # limpia el repositorio\n");
		log.info("[threads=N] # N: cantidad de hilos a ejecutar en paralelo (1 por defecto)");
		log.info("[max=N] # N: cantidad de registros a migrar (ilimitado por defecto)");
		log.info("[dummy=FILE] # FILE: ruta de archivo de prueba a migrar (solo para pruebas)");
	}

	public static void main(String[] args) throws Exception {
		params = parseArgs(args);
		if (params == null) {
			printHelp();
			System.exit(1);
			return;
		}

		if (params.help) {
			printHelp();
			return;
		}

		log.info("Iniciando migracion");

		Properties dbProps = props("/db.properties");

		Properties migradorProps = props("/migrador.properties");
		username = migradorProps.getProperty("username");
		password = migradorProps.getProperty("password");
		repository = migradorProps.getProperty("repository");

		if (params.reset) {
			log.info("Borrando migracion");
			ContainerManager cm = ecm();
			try {
				for (MPDataNode node : cm.retrieveChildDataNodesByPath("/")) {
					cm.removeDataNodeWithDescendants(node.getAbsolutePath());
				}
				for (MPNodeType type : cm.retrieveAllNodeTypes()) {
					cm.removeNodeType(type.getNodeName());
				}
			} finally {
				cm.closeResources();
				DocumentStoreManager.destroyStores();
			}

			try (Connection conn = DriverManager.getConnection(dbProps.getProperty("url"), dbProps)) {
				try (PreparedStatement ps = conn.prepareStatement("delete from dbo.migrados where tocid > 1")) {
					ps.executeUpdate();
				}
			}
			log.info("Finalizado borrado migracion");
			return;
		}

		Runtime.getRuntime().addShutdownHook(new Thread(Main::stop));

		conn = DriverManager.getConnection(dbProps.getProperty("url"), dbProps);

		try (PreparedStatement delete = conn.prepareStatement("delete from dbo.migrados where path is null")) {
			delete.executeUpdate();
		}

		ps = conn.prepareStatement(
				"select t.tocid, t.name, p.path, t.etype from dbo.toc t join dbo.migrados p on p.tocid = t.parentid left join dbo.migrados m on m.tocid = t.tocid where m.tocid is null and p.path is not null /*and t.etype = 0*/");
		rs = ps.executeQuery();
		psInsert = conn.prepareStatement("insert into dbo.migrados (tocid) values (?)");
		psUpdate = conn.prepareStatement("update dbo.migrados set path = ? where tocid = ?");
		psProps = conn.prepareStatement(
				"select d.prop_name, d.prop_type, v.date_val, v.str_val from dbo.propval v join dbo.propdef d on v.prop_id = d.prop_id where v.tocid = ?");
		psPath = conn.prepareStatement(
				"select l.path1, v.fixpath, s.pset_name from dbo.toc t join dbo.activity_log l on t.tocid = l.tocid1 left join dbo.propset s on s.pset_id = t.pset_id join dbo.vol v on v.vol_id = t.vol_id where t.tocid = ?");

		ContainerManager cm = ecm();
		try {
			List<MPNodeType> all = cm.retrieveAllNodeTypes();
			nodeTypes = new HashMap<>();
			for (MPNodeType type : all) {
				nodeTypes.put(type.getNodeName(), type);
			}
			carpeta = nodeTypes.computeIfAbsent("carpeta", k -> {
				MPNodeType nodeType = makeNodeType(k, false);
				cm.loadNodeDataType(nodeType);
				return nodeType;
			});
			archivo = nodeTypes.computeIfAbsent("archivo", k -> {
				MPNodeType nodeType = makeNodeType(k, true);
				cm.loadNodeDataType(nodeType);
				return nodeType;
			});
		} finally {
			cm.closeResources();
		}

		threads = new Thread[params.totalThreads];
		for (int i = 0; i < params.totalThreads; i++) {
			threads[i] = new Thread(Main::hilo);
			threads[i].start();
		}
	}
}
