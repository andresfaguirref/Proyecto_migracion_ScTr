package sdm.migrador;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileNotFoundException;
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

import javax.activation.FileTypeMap;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aspose.words.BreakType;
import com.aspose.words.ConvertUtil;
import com.aspose.words.Document;
import com.aspose.words.DocumentBuilder;
import com.aspose.words.PageSetup;
import com.aspose.words.PdfSaveOptions;
import com.aspose.words.RelativeHorizontalPosition;
import com.aspose.words.RelativeVerticalPosition;
import com.aspose.words.WrapType;
import com.macroproyectos.ecm.ContainerManager;
import com.macroproyectos.ecm.config.ConfigFile;
import com.macroproyectos.ecm.node.MPDataNode;
import com.macroproyectos.ecm.node.MPNodeType;
import com.macroproyectos.ecm.node.MPProperty;
import com.macroproyectos.ecm.store.DocumentStoreManager;
import com.macroproyectos.plantillas.pdf.oo.PDFManager;

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
	static PreparedStatement psTif;

	static int migrados;

	static Object typesLock = new Object();

	static MPNodeType carpeta;
	static MPNodeType archivo;
	static Map<String, MPNodeType> nodeTypes;

	static Pattern regex = Pattern.compile("^_|[^a-zA-Z0-9_]");

	static String username;
	static String password;
	static String repository;
	static String root;

	static Args params;

	static JSONObject json;

	static PdfSaveOptions options = new PdfSaveOptions();

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

	static synchronized Toc findNext() throws SQLException, IOException {
		if (!runnig || noMore || params.maximumRecords > 0 && migrados >= params.maximumRecords) {
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

	static String realPath(String fixpath) {
		fixpath = fixpath.toLowerCase();
		for (String key : json.keySet()) {
			if (fixpath.startsWith(key)) {
				return json.getString(key) + fixpath.substring(key.length());
			}
		}
		throw new IllegalArgumentException("No se encontro firxpath " + fixpath);
	}

	static File path(String fixpath, int storeId, StringBuilder sb, boolean tif) {
		String fileName = Integer.toString(storeId, 16);
		fileName = StringUtils.leftPad(fileName, 8, '0');
		sb.append(fileName.substring(0, 2));
		sb.append(File.separatorChar);
		sb.append(fileName.substring(2, 4));
		sb.append(File.separatorChar);
		sb.append(fileName.substring(4, 6));
		sb.append(File.separatorChar);
		sb.append(fileName);
		if (tif) {
			sb.append(".tif");
		}
		File file = new File(fixpath, sb.toString());
		log.info("Cargando archivo: {}", file.getAbsolutePath());
		return file;
	}

	static void makePdfTif(String fixpath, long tocId, File dst) throws SQLException, IOException {
		psTif.setLong(1, tocId);
		try (ResultSet rs = psTif.executeQuery()) {
			Document doc = new Document();
			DocumentBuilder builder = new DocumentBuilder(doc);
			boolean noFirst = false;
			while (rs.next()) {
				File page = path(fixpath, rs.getInt(1), new StringBuilder(), true);
				if (!page.exists()) {
					throw new FileNotFoundException(page.getAbsolutePath());
				}
				ImageInputStream iis = ImageIO.createImageInputStream(page);
				ImageReader reader = ImageIO.getImageReaders(iis).next();
				reader.setInput(iis, false);
				try {
					int framesCount = reader.getNumImages(true);

					for (int frameIdx = 0; frameIdx < framesCount; frameIdx++) {
						if (noFirst) {
							builder.insertBreak(BreakType.SECTION_BREAK_NEW_PAGE);
						} else {
							noFirst = true;
						}

						BufferedImage image = reader.read(frameIdx);

						PageSetup ps = builder.getPageSetup();

						ps.setPageWidth(ConvertUtil.pixelToPoint(image.getWidth()));
						ps.setPageHeight(ConvertUtil.pixelToPoint(image.getHeight()));

						builder.insertImage(image, RelativeHorizontalPosition.PAGE, 0, RelativeVerticalPosition.PAGE, 0,
								ps.getPageWidth(), ps.getPageHeight(), WrapType.NONE);
					}
				} finally {
					iis.close();
					reader.dispose();
				}
			}
			doc.save(dst.getAbsolutePath(), options);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new MException(e);
		}
	}

	static Toc next() throws SQLException, IOException {
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
					String fixpath = realPath(rs.getString(2));
					toc.ext = rs.getString(1);
					if (toc.ext == null) {
						toc.ext = "pdf";
						toc.fixpath = fixpath;
					} else {
						toc.file = path(fixpath, rs.getInt(4), new StringBuilder("e"), false);
						if (!toc.file.exists()) {
							throw new FileNotFoundException(toc.file.getAbsolutePath());
						}
					}
				} else {
					toc.ext = "pdf";
					toc.file = params.dummyPDF;
				}
				toc.nodeType = rs.getString(3);
			}

			loadTocProps(toc);
		}

		psInsert.setLong(1, toc.id);
		psInsert.executeUpdate();

		return toc;
	}

	static void loadTocProps(Toc toc) throws SQLException {
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

	static void stopECM() {
		DocumentStoreManager.destroyStores();
	}

	static void stop() {
		log.info("Finalizando migracion");
		runnig = false;
		try {
			if (threads != null) {
				for (Thread t : threads) {
					log.info("Esperando hilo {}", t.getName());
					t.join();
					log.info("Finalizado hilo {}", t.getName());
				}
			}
		} catch (InterruptedException e) {
			log.error("", e);
			Thread.currentThread().interrupt();
		}
		close(psTif);
		close(psPath);
		close(psProps);
		close(psUpdate);
		close(psInsert);
		close(rs);
		close(ps);
		close(conn);
		stopECM();
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
			StringBuilder sb;
			int index = 0;
			if (name.charAt(0) == '_') {
				sb = new StringBuilder(name.length() + 1);
				sb.append('A');
			} else {
				sb = new StringBuilder(name.length());
			}
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

	static void process(ContainerManager cm, Toc toc) {
		String name = normalize(toc.name);
		String ruta;
		if ("/".equals(toc.path)) {
			ruta = "/" + name;
		} else {
			ruta = toc.path + "/" + name;
		}

		String threadName = Thread.currentThread().getName();
		try {
			MPNodeType nodeType;
			Map<String, Object> props = new HashMap<>();
			if (toc.isFile()) {
				log.info("Hilo {}, Archivo {}", threadName, ruta);
				nodeType = updateNodeType(cm, toc, props);
			} else {
				log.info("Hilo {}, Carpeta {}", threadName, ruta);
				nodeType = carpeta;
			}
			props.put("tocid", toc.id);
			props.put("nombreOriginal", toc.name);

			MPDataNode dataNode = MPDataNode.createDataNode(name, nodeType);
			dataNode.setProperties(props);
			dataNode.setParent(root + toc.path);

			String temporalFileName = null;
			if (toc.isFile()) {
				String fileName = name + "." + toc.ext;
				temporalFileName = cm.getRandomFileName(fileName);
				File dst = new File(cm.getTemporalFilesFolder(), temporalFileName);
				if (toc.file == null) {
					makePdfTif(toc.fixpath, toc.id, dst);
				} else {
					pdfA(toc.file, dst, toc.ext);
				}
				dataNode.setContentFileName(fileName);
				dataNode.setMimeType(FileTypeMap.getDefaultFileTypeMap().getContentType(fileName));
			}
			cm.persistDataNode(dataNode, temporalFileName);

			update(toc.id, ruta);
		} catch (Exception e) {
			log.error("Fallo {}", ruta);
			log.error("", e);
			if (params.stop) {
				throw new MException(e);
			}
		}
	}

	private static MPNodeType updateNodeType(ContainerManager cm, Toc toc, Map<String, Object> props) {
		MPNodeType nodeType;
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
		return nodeType;
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

				process(cm, toc);
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

	private static void pdfA(File src, File dst, String ext) throws IOException {
		if (ext.equals("pdf")) {
			PDFManager pdf = new PDFManager();
			try {
				pdf.load(src.getAbsolutePath(), ext, dst.getAbsolutePath());
				pdf.convertToPDFA();
			} catch (Exception e) {
				throw new MException(e);
			} finally {
				pdf.close();
			}
		} else {
			Files.copy(src.toPath(), dst.toPath());
		}
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
				case "stop":
					params.stop = Boolean.parseBoolean(value);
					break;
				case "folder":
					params.folder = value;
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
		log.info("[stop=true/false] # Indica si se detiene en caso de fallo o continua con el siguiente");
		log.info("[folder=nombrecarpeta] # Nombre de la carpeta a filtrar");
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

		try (InputStream stream = Main.class.getResourceAsStream("/fs.json")) {
			JSONObject tmp = new JSONObject(new JSONTokener(stream));
			json = new JSONObject();
			for (String key : tmp.keySet()) {
				json.put(key.toLowerCase(), tmp.get(key));
			}
		}

		InputStream lStream = PDFManager.class.getResourceAsStream("/Aspose.Total.Java.lic");
		com.aspose.words.License license = new com.aspose.words.License();
		license.setLicense(lStream);

		options.setCompliance(com.aspose.words.PdfCompliance.PDF_A_1_A);

		Properties dbProps = props("/db.properties");

		Properties migradorProps = props("/migrador.properties");
		username = migradorProps.getProperty("username");
		password = migradorProps.getProperty("password");
		repository = migradorProps.getProperty("repository");
		root = "/" + repository;

		Thread stopECM = new Thread(Main::stopECM);
		Runtime.getRuntime().addShutdownHook(stopECM);

		ConfigFile.loadProperties();

		if (params.reset) {
			log.info("Borrando migracion");
			ContainerManager cm = ecm();
			try {
				for (MPDataNode node : cm.retrieveChildDataNodesByPath(root)) {
					String path = node.getAbsolutePath();
					log.info("Borrando carpeta {}", path);
					cm.removeDataNodeWithDescendants(path);
				}
				for (MPNodeType type : cm.retrieveAllNodeTypes()) {
					String name = type.getNodeName();
					log.info("Borrando tipo {}", name);
					cm.removeNodeType(name);
				}
			} finally {
				cm.closeResources();
			}

			try (Connection conn = DriverManager.getConnection(dbProps.getProperty("url"), dbProps)) {
				try (PreparedStatement ps = conn.prepareStatement("delete from dbo.migrados where tocid > 1")) {
					ps.executeUpdate();
				}
			}
			log.info("Finalizado borrado migracion");
			return;
		}

		Runtime.getRuntime().removeShutdownHook(stopECM);
		Runtime.getRuntime().addShutdownHook(new Thread(Main::stop));

		conn = DriverManager.getConnection(dbProps.getProperty("url"), dbProps);

		try (PreparedStatement delete = conn.prepareStatement("delete from dbo.migrados where path is null")) {
			delete.executeUpdate();
		}

		String sql = "select t.tocid, t.name, p.path, t.etype from dbo.toc t join dbo.migrados p on p.tocid = t.parentid left join dbo.migrados m on m.tocid = t.tocid where m.tocid is null and p.path is not null /*and t.etype = 0*/";
		if (params.folder != null) {
			sql += " and lower(p.path) + '/' + lower(t.name) like ?";
		}
		ps = conn.prepareStatement(sql);
		if (params.folder != null) {
			String param = "%" + params.folder.toLowerCase() + "%";
			ps.setString(1, param);
		}
		rs = ps.executeQuery();
		psInsert = conn.prepareStatement("insert into dbo.migrados (tocid) values (?)");
		psUpdate = conn.prepareStatement("update dbo.migrados set path = ? where tocid = ?");
		psProps = conn.prepareStatement(
				"select d.prop_name, d.prop_type, v.date_val, v.str_val from dbo.propval v join dbo.propdef d on v.prop_id = d.prop_id where v.tocid = ?");
		psPath = conn.prepareStatement(
				"select t.edoc_ext, v.fixpath, s.pset_name, t.edoc_storeid from dbo.toc t left join dbo.propset s on s.pset_id = t.pset_id join dbo.vol v on v.vol_id = t.vol_id where t.tocid = ?");
		psTif = conn.prepareStatement("select storeid from dbo.doc where tocid = ? order by pagenum");

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
