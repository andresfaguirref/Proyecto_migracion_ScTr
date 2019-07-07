package sdm.migrador;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.macroproyectos.ecm.ContainerManager;
import com.macroproyectos.ecm.node.MPDataNode;
import com.macroproyectos.ecm.store.DocumentStoreManager;

public class Main {

	static Logger log = LoggerFactory.getLogger(Main.class.getName());

	static Thread[] threads;
	static boolean runnig = true;
	static boolean noMore;

	static BasicDataSource ds;
	static Connection conn;
	static PreparedStatement ps;
	static ResultSet rs;
	static PreparedStatement psInsert;
	static PreparedStatement psUpdate;

	static int migrados;

	static synchronized Toc findNext() throws SQLException {
		if (!runnig || noMore || migrados > 35) {
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
		toc.created = rs.getDate(3);
		toc.modified = rs.getDate(4);
		toc.path = rs.getString(5);

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
		close(psUpdate);
		close(psInsert);
		close(rs);
		close(ps);
		close(conn);
		close(ds);
		DocumentStoreManager.destroyStores();
		log.info("Total migrados {}", migrados);
		log.info("Migracion finalizada");
	}

	static void hilo() {
		log.info("Iniciando hilo {}", Thread.currentThread().getName());

		ContainerManager cm = new ContainerManager();
		try {
			cm.initializeRepository("admin", "admin", "laserfiche");
			while (runnig) {
				Toc toc = findNext();
				if (toc == null || !runnig) {
					break;
				}

				String name = toc.name.replaceAll("[^a-zA-Z0-9]", "_");
				String ruta;
				if ("/".equals(toc.path)) {
					ruta = "/" + name;
				} else {
					ruta = toc.path + "/" + name;
				}

				log.info("Insertando {}", ruta);

				HashMap<String, Object> props = new HashMap<>();
				props.put("tocid", toc.id);
				// props.put("creado", toc.created)
				// props.put("modificado", toc.modified)
				props.put("nombreOriginal", toc.name);

				MPDataNode dataNode = MPDataNode.createDataNode(name, cm.retrieveNodeType("carpeta"));
				dataNode.setProperties(props);
				dataNode.setParent(toc.path);
				cm.persistDataNode(dataNode);

				update(toc.id, ruta);
			}
		} catch (SQLException e) {
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

		ds = BasicDataSourceFactory.createDataSource(dbProps);
		conn = ds.getConnection();

		try (PreparedStatement delete = conn.prepareStatement("delete from dbo.migrados where path is null")) {
			delete.executeUpdate();
		}

		ps = conn.prepareStatement(
				"select t.tocid, t.name, t.created, t.modified, p.path from dbo.toc t join dbo.migrados p on p.tocid = t.parentid left join dbo.migrados m on m.tocid = t.tocid where m.tocid is null and p.path is not null and t.etype = 0");
		rs = ps.executeQuery();
		psInsert = conn.prepareStatement("insert into dbo.migrados (tocid) values (?)");
		psUpdate = conn.prepareStatement("update dbo.migrados set path = ? where tocid = ?");

		for (Thread t : threads) {
			t.start();
		}
	}
}
