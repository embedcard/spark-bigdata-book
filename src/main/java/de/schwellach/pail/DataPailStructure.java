package de.schwellach.pail;

import com.helixleisure.schema.Data;

/**
 * Adaption of the batch workflow from Nathan Marz
 * (http://twitter.com/nathanmarz) from his book
 * https://www.manning.com/books/big-data
 * 
 * @author Janos Schwellach (http://twitter.com/jschwellach)
 */
public class DataPailStructure extends ThriftPailStructure<Data> {
	private static final long serialVersionUID = -3366429675621884908L;

	@Override
	protected Data createThriftObject() {
		return new Data();
	}

	@SuppressWarnings("rawtypes")
	public Class getType() {
		return Data.class;
	}
}
