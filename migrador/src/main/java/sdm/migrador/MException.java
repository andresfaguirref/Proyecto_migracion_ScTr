package sdm.migrador;

public class MException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	MException(Exception e) {
		super(e);
	}
}
