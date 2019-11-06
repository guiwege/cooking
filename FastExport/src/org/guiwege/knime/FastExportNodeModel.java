package org.guiwege.knime;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;


import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.reader.DBReader;


/**
 *
 * @author Guilherme Wege Chagas
 */
@SuppressWarnings("deprecation")
public class FastExportNodeModel extends NodeModel implements FlowVariableProvider {
    
	private static DatabaseConnectionSettings connSettings = null;
	private static DataTableSpec teradataTableSpec = null;
	
	// Variables for Teradata
    public static final int INJECT_NO_ERROR = 0;
    public static final int INJECT_CONTRAINT_ERROR = 1;
    public static final int INJECT_UNIQUE_PRIMARY_INDEX_ERROR = 2;
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(FastExportNodeModel.class);

	private static final String KEY_BULK_SIZE = "bulk_size";
	private static final String KEY_SELECT_STATEMENT = "select_statement";
	private static final String KEY_ALL_COLUMNS_ARE_STRINGS = "all_columns_are_strings";
	//private static final String KEY_EXPORT_TO_CSV = "export_to_csv";
	//private static final String KEY_PATH_TO_CSV_FILE = "path_to_csv_file";
	
	private static final int DEFAULT_BULK_SIZE = 0;
	private static final String DEFAULT_SELECT_STATEMENT = "SELECT *\nFROM $${STABLE_NAME_VAR}$$\n;";
	private static final boolean DEFAULT_ALL_COLUMNS_ARE_STRINGS = false;
	//private static final boolean DEFAULT_EXPORT_TO_CSV = false;
	//private static final String DEFAULT_PATH_TO_CSV_FILE = "";
	
	private final SettingsModelInteger m_bulkSizeSettings = createBulkSizeSettingsModel();
	private final SettingsModelString m_selectStatementSettings = createSelectStatementSettingsModel();
	private final SettingsModelBoolean m_allColumnsAreStrings = createAllColumnsAreStringsSettingsModel();
	//private final SettingsModelBoolean m_exportToCSVSettings = createExportToCSVSettingsModel();
	//private final SettingsModelString m_pathToCSVFileSettings = createPathToCSVFileSettingsModel();
	
	protected FastExportNodeModel() {
		super(new PortType[] {DatabaseConnectionPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE});
	}

	static SettingsModelInteger createBulkSizeSettingsModel() {
		return new SettingsModelInteger(KEY_BULK_SIZE, DEFAULT_BULK_SIZE);
	}
	
	static SettingsModelString createSelectStatementSettingsModel() {
		return new SettingsModelString(KEY_SELECT_STATEMENT, DEFAULT_SELECT_STATEMENT);
	}
	
	static SettingsModelBoolean createAllColumnsAreStringsSettingsModel() {
		return new SettingsModelBoolean(KEY_ALL_COLUMNS_ARE_STRINGS, DEFAULT_ALL_COLUMNS_ARE_STRINGS);
	}

	/*
	static SettingsModelBoolean createExportToCSVSettingsModel() {
		return new SettingsModelBoolean(KEY_EXPORT_TO_CSV, DEFAULT_EXPORT_TO_CSV);
	}
	
	static SettingsModelString createPathToCSVFileSettingsModel() {
		return new SettingsModelString(KEY_PATH_TO_CSV_FILE, DEFAULT_PATH_TO_CSV_FILE);
	}
	*/
    
	
	
	/**
	 * 
	 * {@inheritDoc}
	 */	
	@Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

		DatabaseConnectionPortObjectSpec incomingConnection = (DatabaseConnectionPortObjectSpec)inSpecs[0];
        connSettings = incomingConnection.getConnectionSettings(getCredentialsProvider());
		
		try {
			DataTableSpec resultSpec = getResultSpec(inSpecs);
			
	        if (resultSpec == null) {
	        	LOGGER.error("null resultSpec");
	        }
	        teradataTableSpec = resultSpec;
	        
	        StringBuilder sbColunas = new StringBuilder();
        	String separador = "";
	        
	        ListIterator<String> it = Arrays.asList(resultSpec.getColumnNames()).listIterator();
			while (it.hasNext()) {
				int index = it.nextIndex();
				String item = it.next(); 
				
				if (index > 0) {
					separador = ",";
				}
				sbColunas.append(separador + item);
			}
			
	        LOGGER.info("Colunas: " + sbColunas);
	        
	        if (m_allColumnsAreStrings.getBooleanValue()) {
	        	LOGGER.info("Config \"set All Columns Are Strings\" to true");
	        	int columnsCount = resultSpec.getNumColumns();
	        	
	        	// Array with all data types set to StringCell
	        	DataType stringTypesArray[] = new DataType[columnsCount];
	        	Arrays.fill(stringTypesArray, StringCell.TYPE);
	        	
	        	DataTableSpec allColumnsAreStringsTableSpec = new DataTableSpec(resultSpec.getColumnNames(), stringTypesArray);
	        	teradataTableSpec = allColumnsAreStringsTableSpec;
	        	return new DataTableSpec[] {allColumnsAreStringsTableSpec};
	        }
	        else {
				return new DataTableSpec[]{resultSpec};	
	        }
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
        return new DataTableSpec[] {null};
	}
	
	
    @Override
    protected PortObject[] execute(final PortObject[] inData, ExecutionContext exec) throws CanceledExecutionException, Exception {	
        
    	// Create a container for the datatypes identified in the configure method
		BufferedDataContainer container = exec.createDataContainer(teradataTableSpec);
		
		
        try {
        	// Loads the Teradata JDBC Driver
			Class.forName("com.teradata.jdbc.TeraDriver");
			
			String sql = FlowVariableResolver.parse(m_selectStatementSettings.getStringValue(), this);
			//String sql = m_selectStatementSettings.getStringValue();
			String select = sql;

			// Ensure that TYPE=FASTEXPORT is set
			String jdbc = removeAtributoJDBC(connSettings.getJDBCUrl(), "TYPE=FASTEXPORT");
			// 
			Connection con = DriverManager.getConnection(jdbc + ",TYPE=FASTEXPORT", connSettings.getUserName(), connSettings.getPassword());
			con.clearWarnings();

			LOGGER.info("Beginning FastExport");
			
			PreparedStatement pstmt = con.prepareStatement(select);
			
			// Show warnings
			printSQLWarnings(con);
			
			// Reads the ResultSet
			ResultSet rs = pstmt.executeQuery();
			rs.setFetchSize(m_bulkSizeSettings.getIntValue());
            ResultSetMetaData rsmd = rs.getMetaData();

            // Let's create some datetime formats
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");
            SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            
			int rowsSelected = 0;
			
			for (int i = 1; rs.next(); i++, rowsSelected++) {
				
				List<DataCell> cells = new ArrayList<>();
				
                for (int j = 1; j <= rsmd.getColumnCount(); j++) {
                	
                	if (rs.getObject(j) == null) {
                		cells.add(new MissingCell("?"));
                	} 
                	else {
                		String tipoTeradata = rsmd.getColumnTypeName(j).toUpperCase();
                    	
                    	switch(tipoTeradata) {
                    		case "SMALLINT":
                    		case "INTEGER":
                    		case "BYTEINT":
                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(rs.getString(j)));
                    			} else {
                        			cells.add(new IntCell(rs.getInt(j)));
                    			}
                    			break;
                    		case "BIGINT":
                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(rs.getString(j)));
                    			} else {
                        			cells.add(new LongCell(rs.getLong(j)));
                    			}
                    			break;
                    		case "DECIMAL":
                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(rs.getBigDecimal(j).toPlainString()));
                    			} 
                    			else {
                    				BigDecimal bd = rs.getBigDecimal(j);
                        			cells.add(new DoubleCell(bd.doubleValue()));
                    			}
                    			break;
                    		case "FLOAT":
                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(rs.getBigDecimal(j).toPlainString()));
                    			} else {
                        			cells.add(new DoubleCell(rs.getDouble(j)));
                    			}
                    			break;
                    		case "VARCHAR":
                    		case "CHAR":
                    			//cells.add(new StringCell((String) rs.getObject(j)));
                    			cells.add(new StringCell(rs.getString(j)));
                    			break;
                    		case "DATE":
                    			Date dt = rs.getDate(j);

                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(dateFormat.format(dt)));
                    			} else {
                        			// It's necessary to add 1900 to the year because 
                        			// the driver subtracts 1900 from it
                        			cells.add(new DateAndTimeCell(dt.getYear() + 1900, dt.getMonth(), dt.getDate()));
                    			}
                    			break;
                    		case "TIME":
                    			Time time = rs.getTime(j);
                    			
                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(timeFormat.format(time)));
                    			} else {
                        			cells.add(new DateAndTimeCell(time.getHours(), time.getMinutes(), time.getSeconds(), 0));
                    			}
                    			break;
                    		case "TIMESTAMP":
                    			Timestamp timestamp = rs.getTimestamp(j);

                    			if (m_allColumnsAreStrings.getBooleanValue()) {
                    				cells.add(new StringCell(timestampFormat.format(timestamp)));
                    			} else {
                    				cells.add(new DateAndTimeCell(timestamp.getYear() + 1900, timestamp.getMonth(),
                        					timestamp.getDate(), timestamp.getHours(), timestamp.getMinutes(),
                        					timestamp.getSeconds(), 0));	
                    			}
                    			break;
                    		default:
                    			cells.add(new StringCell(rs.getObject(j).toString()));
                    	}
                	}
                }
                // Inserts a row to the KNIME table
                DataRow row = new DefaultRow(RowKey.createRowKey(rowsSelected), cells);
                
                container.addRowToTable(row);
                
                exec.setMessage("Row: " + String.valueOf(i));
                
                if (i % 1000000 == 0 ) {
                	// Show progress for each 1,000,000 rows
                    LOGGER.info(i + " rows processed...");	
                }
                
                // Check if the execution was cancelled by the user
                exec.checkCanceled();
            }
			rs.close();
		} catch (SQLException e) {
			printStackTraceToLog(e);
		} 

		container.close();
		BufferedDataTable out = container.getTable();
		return new BufferedDataTable[] { out };
    }


    
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		/*
		 * Save user settings to the NodeSettings object. SettingsModels already know how to
		 * save them self to a NodeSettings object by calling the below method. In general,
		 * the NodeSettings object is just a key-value store and has methods to write
		 * all common data types. Hence, you can easily write your settings manually.
		 * See the methods of the NodeSettingsWO.
		 */
		m_selectStatementSettings.saveSettingsTo(settings);
		m_allColumnsAreStrings.saveSettingsTo(settings);
		m_bulkSizeSettings.saveSettingsTo(settings);
		//m_exportToCSVSettings.saveSettingsTo(settings);
		//m_pathToCSVFileSettings.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		/*
		 * Load (valid) settings from the NodeSettings object. It can be safely assumed that
		 * the settings are validated by the method below.
		 * 
		 * The SettingsModel will handle the loading. After this call, the current value
		 * (from the view) can be retrieved from the settings model.
		 */
		m_selectStatementSettings.loadSettingsFrom(settings);
		m_allColumnsAreStrings.loadSettingsFrom(settings);
		m_bulkSizeSettings.loadSettingsFrom(settings);
		//m_exportToCSVSettings.loadSettingsFrom(settings);
		//m_pathToCSVFileSettings.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		/*
		 * Check if the settings could be applied to our model e.g. if the user provided
		 * format String is empty. In this case we do not need to check as this is
		 * already handled in the dialog. Do not actually set any values of any member
		 * variables.
		 */
		m_selectStatementSettings.validateSettings(settings);
		m_allColumnsAreStrings.validateSettings(settings);
		m_bulkSizeSettings.validateSettings(settings);
		//m_exportToCSVSettings.validateSettings(settings);
		//m_pathToCSVFileSettings.validateSettings(settings);
	}

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		/*
		 * Advanced method, usually left empty. Everything that is
		 * handed to the output ports is loaded automatically (data returned by the execute
		 * method, models loaded in loadModelContent, and user settings set through
		 * loadSettingsFrom - is all taken care of). Only load the internals
		 * that need to be restored (e.g. data used by the views).
		 */
	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		/*
		 * Advanced method, usually left empty. Everything
		 * written to the output ports is saved automatically (data returned by the execute
		 * method, models saved in the saveModelContent, and user settings saved through
		 * saveSettingsTo - is all taken care of). Save only the internals
		 * that need to be preserved (e.g. data used by the views).
		 */
	}

	@Override
	protected void reset() {
		/*
		 * Code executed on a reset of the node. Models built during execute are cleared
		 * and the data handled in loadInternals/saveInternals will be erased.
		 */
	}
	
	
	// Custom functions
	private static String removeAtributoJDBC(String jdbc, String atrib) {
		// Removes the specified attribute from the string
		String[] tokens = jdbc.split(",");
		
		StringBuilder sbTokens = new StringBuilder();
		
		for (int i=0;i < tokens.length; i++) {
			
			if (! tokens[i].toUpperCase().trim().equals(atrib.toUpperCase())) {
				if (i > 0) {
					sbTokens.append(",");	
				}
				sbTokens.append(tokens[i]);
			}
		}
		return sbTokens.toString();
	}
	
	private void printStackTraceToLog(Exception e) throws InvalidSettingsException  {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true));
		LOGGER.debug(sw);
		
		if (e instanceof SQLException) {
			String s = e.getMessage();
			  while (((SQLException) e).getNextException() != null) {
			    e = ((SQLException) e).getNextException();
			    s += ", \ndue to: " + e.getMessage();
			  }
			  LOGGER.error(s, e);
			  throw new InvalidSettingsException("Node error: See above");
		}
		else {
			throw new InvalidSettingsException(
					"Node error: " + e.getMessage(), e);
		}
	}
	
	private void printSQLWarnings(Connection con) throws SQLException {
		SQLWarning w = con.getWarnings();
		while (w != null) {
        	LOGGER.warn("*** SQLWarning caught ***");
            StringWriter sw = new StringWriter();
            w.printStackTrace(new PrintWriter(sw, true));
            LOGGER.warn("SQL State = " + w.getSQLState()
                + ", Error Code = " + w.getErrorCode() +
                "\n" + sw.toString());
            w = w.getNextWarning();
        }
		con.clearWarnings();
	}

    // Returns a DataTableSpec with the data types
	protected DataTableSpec getResultSpec(final PortObjectSpec[] inSpecs)
	        throws InvalidSettingsException, SQLException {

			FlowVariableResolver.parse(m_selectStatementSettings.getStringValue(), this);
	        //String query = m_selectStatementSettings.getStringValue();
			String query = FlowVariableResolver.parse(m_selectStatementSettings.getStringValue(), this);
	    
            DatabaseConnectionPortObjectSpec connSpec =
                (DatabaseConnectionPortObjectSpec)inSpecs[getNrInPorts() - 1];
            DatabaseQueryConnectionSettings connSettings = new DatabaseQueryConnectionSettings(
                connSpec.getConnectionSettings(getCredentialsProvider()), query);
        
	        DBReader reader = connSettings.getUtility().getReader(connSettings);
	        DataTableSpec resultSpec = reader.getDataTableSpec(getCredentialsProvider());
	        return resultSpec;
	}
}

