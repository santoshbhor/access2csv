package access2csv;
import com.healthmarketscience.jackcess.DataType;

public class RowDataImportError {
    public int RowNum;
	public String RowsAsCsv;
    public int ColumnNum;
	public String ColumnName;
    public String ColumnValue;
    public DataType DataTypeDefined;
    public String ErrorMessage;
    
    public Boolean hasErrors()
    {
        if(ErrorMessage == null) return false;
        if(ErrorMessage.isBlank() || ErrorMessage.isEmpty()) return false;
        
        return true;
    }

    public void printErrorsToConsole()
    {
        if(hasErrors())
        {
            System.out.println("ERROR ROW(" + RowNum + ")\nROW DATA -> " + RowsAsCsv);
            System.out.println("ERROR (" + ColumnName + ") -> VALUE -> " + ColumnValue + " -> ERROR -> "  + ErrorMessage);
        }
    }

}
