package org.guiwege.knime.fastload;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import org.knime.core.data.MissingCell;
import java.util.ArrayList;
import java.util.List;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
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
 * @author Guilherme Wege Chagas
 */
@SuppressWarnings("deprecation")
public class FastLoadNodeModel extends NodeModel {

	private static DatabaseConnectionSettings connSettings = null;

    // Variables for the data types
    private static List<String> tableColumnsDataTypes = null;
    private static List<String> tableColumnsNames = null;
	
    // Debug variables
    // The current row being processed
    private int currentRowNumberForDebug = 0;
    // The current cell value being processed for debugging, in string format
    private String currentCellValueForDebug = "";
    
	private static final NodeLogger LOGGER = NodeLogger.getLogger(FastLoadNodeModel.class);

	private static final String KEY_TABLE_NAME = "table_name";
	private static final String KEY_BULK_SIZE = "bulk_size";

	private static final String DEFAULT_TABLE_NAME = "";
	private static final int DEFAULT_BULK_SIZE = 1000000;

	private final SettingsModelString m_tableNameSettings = createTableNameSettingsModel();
	private final SettingsModelInteger m_bulkSizeSettings = createBulkSizeSettingsModel();

	protected FastLoadNodeModel() {

		super(new PortType[] {DatabaseConnectionPortObject.TYPE, BufferedDataTable.TYPE}, new PortType[0]);
	}

	static SettingsModelString createTableNameSettingsModel() {
		return new SettingsModelString(KEY_TABLE_NAME, DEFAULT_TABLE_NAME);
	}
	static SettingsModelInteger createBulkSizeSettingsModel() {
		return new SettingsModelInteger(KEY_BULK_SIZE, DEFAULT_BULK_SIZE);
	}

	/**
	 * 
	 * {@inheritDoc}
	 * @throws InvalidSettingsException 
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws InvalidSettingsException {

		BufferedDataTable inputTable = (BufferedDataTable) inData[1];
		CloseableRowIterator rowIterator = inputTable.iterator();

		try {
        	// Loads the Teradata Driver
	    	Class.forName("com.teradata.jdbc.TeraDriver");
	    	
			String jdbc = removeJDBCAttribute(connSettings.getJDBCUrl(), "TYPE=FASTLOAD");
			Connection con = DriverManager.getConnection(jdbc + ",TYPE=FASTLOAD", connSettings.getUserName(), connSettings.getPassword());
			
			String tableName = m_tableNameSettings.getStringValue();
            
			// Each column is represented by a ?
            String questionMarks = new String(new char[tableColumnsDataTypes.size()]).replace("\0", "?,");
    		questionMarks = questionMarks.substring(0, questionMarks.length()-1); // remove a ultima virgula
    		System.out.println(questionMarks);
            
            String insertStatement = "INSERT INTO " + tableName + " VALUES(" + questionMarks + ")";
            
            PreparedStatement pstmt = con.prepareStatement(insertStatement);
            
            // Show warnings, if any
            printSQLWarnings(con);
            
            int batchCount = 0;
            con.setAutoCommit(false);
            
    		// Iterate over the rows of the input table.
    		while (rowIterator.hasNext()) {
    			DataRow currentRow = rowIterator.next();
    			int numberOfCells = currentRow.getNumCells();
    			
    			// For each column (question mark) in the statement, fill the right value
    			for (int index = 0; index < numberOfCells; index++) { // Starts at 0

    				// But the SQL index starts at 1 
    				int sqlIndex = index+1;
    				
    				DataCell cell = currentRow.getCell(index);
    				String type = tableColumnsDataTypes.get(index);
    				
    				boolean isCurrentCellNull = cell.getClass() == MissingCell.class;

    				// Set the debug cell value
    				currentCellValueForDebug = cell.toString();
    				
					switch(type) {
            		case "SMALLINT":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.SMALLINT);
            			
            			else {
            				IntCell intCell = (IntCell) cell;
            				pstmt.setInt(sqlIndex, intCell.getIntValue());
            			}
            			break;
            		case "INTEGER":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.INTEGER);
            			
            			else {
            				IntCell intCell = (IntCell) cell;
            				pstmt.setInt(sqlIndex, intCell.getIntValue());
            			}
            			break;
            		case "BYTEINT":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.TINYINT);
            			
            			else {
            				IntCell intCell = (IntCell) cell;
            				pstmt.setInt(sqlIndex, intCell.getIntValue());
            			}
            			break;
            		case "BIGINT":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.BIGINT);
            			else {
                			LongCell longCell = (LongCell) cell;
            				pstmt.setLong(sqlIndex, longCell.getLongValue());
            			}
            			break;
            		case "DECIMAL":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.DECIMAL);
            			else {
                			DoubleCell doubleCell = (DoubleCell) cell;
            				pstmt.setDouble(sqlIndex, doubleCell.getDoubleValue());
            			}
            			break;
            		case "FLOAT":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.FLOAT);
            			else {
            				// KNIME does not have a float equivalent
                			DoubleCell floatDoubleCell = (DoubleCell) cell;
            				pstmt.setDouble(sqlIndex, floatDoubleCell.getDoubleValue());
            			}
            			break;
            		case "VARCHAR":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.VARCHAR);
            			else {
                			StringCell stringCell = (StringCell) cell;
            				pstmt.setString(sqlIndex, stringCell.getStringValue());
            			}
            			break;
            		case "CHAR":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.CHAR);
            			else {
                			StringCell charCell = (StringCell) cell;
            				pstmt.setString(sqlIndex, charCell.getStringValue());
            			}
            			break;
            		case "DATE":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.DATE);
            			else {
                			DateAndTimeCell dateCell = (DateAndTimeCell) cell;
                			Date date = new Date(dateCell.getYear() - 1900, dateCell.getMonth(), dateCell.getDayOfMonth());
            				pstmt.setDate(sqlIndex, date);
            			}
            			break;
            		case "TIME":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.TIME);
            			else {
                			DateAndTimeCell timeCell = (DateAndTimeCell) cell;
                			Time time = new Time(timeCell.getHourOfDay(), timeCell.getMinute(), timeCell.getSecond());
            				pstmt.setTime(sqlIndex, time);
            			}
            			break;
            		case "TIMESTAMP":
            			if (isCurrentCellNull) pstmt.setNull(sqlIndex, java.sql.Types.TIMESTAMP);
            			else {
                			DateAndTimeCell timestampCell = (DateAndTimeCell) cell;
                			Timestamp timestamp = new Timestamp(timestampCell.getYear() - 1900, 
                					timestampCell.getMonth(),
                					timestampCell.getDayOfMonth(),
                					timestampCell.getHourOfDay(),
                					timestampCell.getMinute(),
                					timestampCell.getSecond(),
                					0);
            				pstmt.setTimestamp(sqlIndex, timestamp);
            			}
            			break;
            		default:
            			LOGGER.error("Coluna " + tableColumnsNames.get(index) + ", na pos: "+ index + ", n�o inserida... Valor: " + cell.toString());
    				}
    			} // end for

    			// Adds a row to the prepared statement
    			pstmt.addBatch();
    			
				batchCount++;
				
				currentRowNumberForDebug = batchCount;
				
				// When there are as many batches as the bulkSizeSettings, 
				// Execute them into the database
				// This is not a commit
				int bulkSize = m_bulkSizeSettings.getIntValue();
				if (batchCount % bulkSize == 0) {
					
					int updateCounts[] = pstmt.executeBatch();
					
                    if (updateCounts == null) {
                        LOGGER.warn(
                            "ERROR: A null update count was returned!");
                    } else {
                        if (updateCounts.length != batchCount) {
                        	LOGGER.warn(
                                "WARNING: The update count does not match the"
                                + " number of rows batched: expected "
                                + batchCount + ", got " + updateCounts.length);
                        }
                        for (int i = 0; i < updateCounts.length; i++) {
                            if (updateCounts[i] != 1) {
                            	LOGGER.warn(
                                    "WARNING: The update count for row " + (i+1)
                                    + " failed: expected 1, got "
                                    + updateCounts[i]);
                            }
                        }
                    }
                    LOGGER.info(batchCount + " rows processed...");	

                    // Check SQLWarning after executeBatch().
                    printSQLWarnings(con);
				}
	    		exec.checkCanceled();
	    		exec.setProgress(batchCount / (double) inputTable.size(), "Last row processed: " + batchCount);
	    		
	    	} // end while
    		
    		// Finished iterating through the rows
    		
    		// Executes the batch again with the remaining rows
			int bulkSize = m_bulkSizeSettings.getIntValue();
    		int updateCounts[] = pstmt.executeBatch();
            if (updateCounts == null) {
                LOGGER.warn(
                    "ERROR: A null update count was returned!");
            } else {
                if (updateCounts.length != bulkSize) {
                	LOGGER.warn(
                        "WARNING: The update count does not match the"
                        + " number of rows batched: expected "
                        + batchCount + ", got " + updateCounts.length);
                }
                for (int i = 0; i < updateCounts.length; i++) {
                    if (updateCounts[i] != 1) {
                    	LOGGER.warn(
                            "WARNING: The update count for row " + (i+1)
                            + " failed: expected 1, got "
                            + updateCounts[i]);
                    }
                }
            }
            LOGGER.info(batchCount + " total rows processed...");	

            LOGGER.info("Commit.");
    		con.commit();
    		printSQLWarnings(con);
    		LOGGER.info("Closing PreparedStatement.");
    		pstmt.close();
    		LOGGER.info("Closing Connection.");
    		con.close();
    		
		} catch (SQLException e) {
			
			printStackTraceToLog(e);

			SQLException ex = e.getNextException();
			StringWriter sw = new StringWriter();
	        ex.printStackTrace(new PrintWriter(sw, true));
			LOGGER.error(sw);
			
		} catch (CanceledExecutionException e) {
			printStackTraceToLog(e);
		} catch (ClassNotFoundException e) {
			printStackTraceToLog(e);
		} 

		// Return an empty BufferedTable
		return new BufferedDataTable[] {};
	}

	protected DataTableSpec getResultSpec(final PortObjectSpec[] inSpecs)
	        throws InvalidSettingsException, SQLException {
	        String query = "SELECT * FROM " + m_tableNameSettings.getStringValue() + ";";
	    
            DatabaseConnectionPortObjectSpec connSpec =
                (DatabaseConnectionPortObjectSpec)inSpecs[0];
            DatabaseQueryConnectionSettings connSettings = new DatabaseQueryConnectionSettings(
                connSpec.getConnectionSettings(getCredentialsProvider()), query);
        
	        DBReader reader = connSettings.getUtility().getReader(connSettings);
	        DataTableSpec resultSpec = reader.getDataTableSpec(getCredentialsProvider());
	        return resultSpec;
	    }
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		
		// Reset variables
	    tableColumnsDataTypes = new ArrayList<>();
	    tableColumnsNames = new ArrayList<>();
		
	    // Checks if table exists in Teradata and if it does, get its specs
		DatabaseConnectionPortObjectSpec incomingConnection = (DatabaseConnectionPortObjectSpec)inSpecs[0];
        connSettings = incomingConnection.getConnectionSettings(getCredentialsProvider());
		
        try {
	    	// Removes TYPE=FASTLOAD if it is present
			String jdbc = removeJDBCAttribute(connSettings.getJDBCUrl(), "TYPE=FASTLOAD");
			
			// This connection is only used for getting the data types
			// No need to call Teradata's drivers here
			Connection con = DriverManager.getConnection(jdbc, connSettings.getUserName(), connSettings.getPassword());
			
			String tableName = m_tableNameSettings.getStringValue();
			
			String select = "SELECT * FROM " + tableName + ";";
			
			PreparedStatement pstmt = con.prepareStatement(select);
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			
	        for (int j = 1; j <= rsmd.getColumnCount(); j++) {
	        	tableColumnsDataTypes.add(rsmd.getColumnTypeName(j).toUpperCase());
	        	tableColumnsNames.add(rsmd.getColumnName(j).toUpperCase());
	        	LOGGER.info("Column " + j + ": " + rsmd.getColumnName(j) + "(" + rsmd.getColumnType(j) + ", " + rsmd.getColumnTypeName(j) + ")");
	        }
	        
	        rs.close();
	        pstmt.close();
	        con.close();
		} catch (SQLException e) {
			printStackTraceToLog(e);
		} 
        
        return new PortObjectSpec[] { };
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
		m_tableNameSettings.saveSettingsTo(settings);
		m_bulkSizeSettings.saveSettingsTo(settings);
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
		m_tableNameSettings.loadSettingsFrom(settings);
		m_bulkSizeSettings.loadSettingsFrom(settings);
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
		m_tableNameSettings.validateSettings(settings);
		m_bulkSizeSettings.validateSettings(settings);
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
	
	
	// Custom methods
	
	private static String removeJDBCAttribute(String jdbc, String atrib) {
		
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
			  LOGGER.error("Row: " + String.valueOf(currentRowNumberForDebug) + ", Value: " + currentCellValueForDebug);
			  throw new InvalidSettingsException("Node error: See above");
		}
		else {
			LOGGER.error("Row: " + String.valueOf(currentRowNumberForDebug) + ", Value: " + currentCellValueForDebug);
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
}

