package gov.acwi.wqp.etl;


public abstract class BaseProcessor {
	
	protected boolean isNonNullAndEqual(String str1, String str2) {
		/* returns true if str1 and str2 are non null and there contents are equal */
		return (str1 == null || str2 == null) ? false : str1.contentEquals(str2);
	}

}
