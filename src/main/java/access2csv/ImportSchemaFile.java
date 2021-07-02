package access2csv;

import java.sql.Types;

import com.healthmarketscience.jackcess.DataType;

public class ImportSchemaFile {
    private String column;
	private String datatype;

	public DataType defaultDataType = DataType.MEMO;
	
	public String getcolumn() {
		return column;
	}

	public void setcolumn(String column) {
		this.column = column;
	}

	public String getdatatype() {
		return datatype;
	}

	public void setdatatype(String datatype) {
		this.datatype = datatype;
	}

	@Override
	public String toString() {
		return "{" + column + "::" + datatype + "}";
	}

	public String toCsv() {
		return column + "," + datatype;
	}

	public Boolean isHeader(){
		return column.equalsIgnoreCase("column") && datatype.equalsIgnoreCase("datatype");
	}

	public DataType toAccessDataType()
	{
		DataType rettype = defaultDataType;
		try{
			rettype = resolveDataType(this, defaultDataType);
		}catch(Exception ex)
		{
			System.out.println("Error resolving DataType for: " + toString() + "returning " + rettype);
		}

		return rettype;
	}

	private static DataType resolveDataType(final ImportSchemaFile inputSchemarow, DataType defaultType) throws Exception {

		DataType _retType = DataType.MEMO; //returned data type to the users
		if(defaultType != null)
			_retType = defaultType;
		
		//Check if the type is a MDb type
		String dtype = inputSchemarow.getdatatype().trim().toUpperCase();
		//String dlength = inputSchemarow.getlength();
		Boolean _found = false;

		//try resolving as mdb type
		try{
			_retType = DataType.valueOf(dtype);
			_found = true;
		}catch(Exception ex)
		{
			_found = false;
		}

		//try resolving as sql type
		if(!_found)
		{
			try{
			//try to check it from java.sql.Types
			java.lang.reflect.Field sqlTypeField = Types.class.getField(dtype);
      		Integer sqltype = (Integer)sqlTypeField.get(null);
			_retType = DataType.fromSQLType(sqltype);
			_found = true;
			}catch(Exception ex)
			{
				_found = false;
			}

		}	
		
		if(!_found)
		{
			System.out.println("(" + inputSchemarow.getcolumn() + ") -> "+ dtype +" -> incorrect! -> using -> " + defaultType);
		}
	
		return _retType;

	}
}
