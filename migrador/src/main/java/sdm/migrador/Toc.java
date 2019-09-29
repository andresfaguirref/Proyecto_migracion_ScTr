package sdm.migrador;

import java.io.File;
import java.util.Map;

public class Toc {
	long id;
	String name;
	String path;
	int type;
	String ext;
	String fixpath;
	File file;
	String nodeType;
	Map<String, Object> properties;

	boolean isFile() {
		return type == -2;
	}
}
