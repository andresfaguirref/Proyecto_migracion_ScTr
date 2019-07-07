package sdm.migrador;

import java.io.File;
import java.io.IOException;
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

	static synchronized Toc findNext() throws SQLException {
		if (!runnig || noMore || migrados > 100) {
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
				toc.file = new File(rs.getString(2), rs.getString(1));
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

	static void hilo() {
		log.info("Iniciando hilo {}", Thread.currentThread().getName());

		ContainerManager cm = new ContainerManager();
		try {
			cm.initializeRepository("admin", "admin", "laserfiche");
			List<MPNodeType> all = cm.retrieveAllNodeTypes();
			MPNodeType carpeta = null;
			Map<String, MPNodeType> nodeTypes = new HashMap<>();
			for (MPNodeType type : all) {
				if (carpeta == null && "carpeta".equals(type.getNodeName())) {
					carpeta = type;
				} else {
					nodeTypes.put(type.getNodeName(), type);
				}
			}
			if (carpeta == null) {
				throw new IllegalStateException("No existe el tipo de nodo 'carpeta'");
			}
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
					log.info("Insertando archivo {}", ruta);
					synchronized (typesLock) {
						nodeType = nodeTypes.get(toc.nodeType);
						if (nodeType == null) {
							String nodeName = normalize(toc.nodeType);

							nodeType = new MPNodeType();
							nodeType.setFile(true);
							nodeType.setNodeName(nodeName);
							nodeType.setMixin(false);

							checkNodeTypeProperties(nodeType, props, toc.properties);
							cm.loadNodeDataType(nodeType);

							nodeTypes.put(toc.nodeType, nodeType);
						} else {
							if (checkNodeTypeProperties(nodeType, props, toc.properties)) {
								cm.updateNodeType(nodeType);
							}
						}
					}
				} else {
					log.info("Insertando carpeta {}", ruta);

					nodeType = carpeta;
					props.put("tocid", toc.id);
					props.put("nombreOriginal", toc.name);
				}
				MPDataNode dataNode = MPDataNode.createDataNode(name, nodeType);
				dataNode.setProperties(props);
				dataNode.setParent(toc.path);

				if (toc.isFile()) {
					dataNode.setContent(Files.readAllBytes(toc.file.toPath()));
					dataNode.setContentFileName(name + ".pdf");
					dataNode.setMimeType("application/pdf");
				}
				cm.persistDataNode(dataNode);

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

		log.info("Finalizando hilo {}", Thread.currentThread().getName());
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 1) {
			log.error("Parametros incorrectos");
			System.exit(1);
			return;
		}

		log.info("Iniciando migracion");

		int total = 1;
		if (args != null && args.length == 1) {
			total = Integer.parseInt(args[0]);
			if (total < 1) {
				throw new IllegalArgumentException(args[0]);
			}
		}

		Properties dbProps = new Properties();
		dbProps.load(Main.class.getResourceAsStream("/db.properties"));

		threads = new Thread[total];
		for (int i = 0; i < total; i++) {
			threads[i] = new Thread(Main::hilo);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(Main::stop));

		conn = DriverManager.getConnection(dbProps.getProperty("url"), dbProps);

		try (PreparedStatement delete = conn.prepareStatement("delete from dbo.migrados where path is null")) {
			delete.executeUpdate();
		}

		ps = conn.prepareStatement(
				"select t.tocid, t.name, p.path, t.etype from dbo.toc t join dbo.migrados p on p.tocid = t.parentid left join dbo.migrados m on m.tocid = t.tocid where m.tocid is null and p.path is not null");
		rs = ps.executeQuery();
		psInsert = conn.prepareStatement("insert into dbo.migrados (tocid) values (?)");
		psUpdate = conn.prepareStatement("update dbo.migrados set path = ? where tocid = ?");
		psProps = conn.prepareStatement(
				"select d.prop_name, d.prop_type, v.date_val, v.str_val from dbo.propval v join dbo.propdef d on v.prop_id = d.prop_id where v.tocid = ?");
		psPath = conn.prepareStatement(
				"select l.path1, v.fixpath, s.pset_name from dbo.toc t join dbo.activity_log l on t.tocid = l.tocid1 join dbo.propset s on s.pset_id = t.pset_id join dbo.vol v on v.vol_id = t.vol_id where t.tocid = ?");

		for (Thread t : threads) {
			t.start();
		}
	}

	static boolean checkNodeTypeProperties(MPNodeType nodeType, Map<String, Object> values, Map<String, Object> props) {
		Map<String, MPProperty> currentProps = nodeType.getProperties();
		boolean modified = false;
		for (Entry<String, Object> property : props.entrySet()) {
			String name = normalize(property.getKey());
			if (!currentProps.containsKey(name)) {
				MPProperty prop = new MPProperty();
				prop.setName(name);
				prop.setDataType(property.getValue() instanceof String ? MPProperty.STRING : MPProperty.DATE);
				prop.setAutocreated(false);
				prop.setMandatory(false);
				prop.setMultiple(false);
				prop.setValue(null);
				nodeType.addProperty(prop);
				modified = true;
			}
			values.put(name, property.getValue());
		}
		return modified;
	}

	static String normalize(String name) {
		return name.replaceAll("[^a-zA-Z0-9]", "_");
	}
}
