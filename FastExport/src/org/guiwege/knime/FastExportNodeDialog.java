package org.guiwege.knime;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/** 
 * @author Guilherme Wege Chagas
 */
public class FastExportNodeDialog extends DefaultNodeSettingsPane {

    protected FastExportNodeDialog() {
        super();
        

        SettingsModelInteger integerSettings = FastExportNodeModel.createBulkSizeSettingsModel();
        SettingsModelString stringSettings = FastExportNodeModel.createSelectStatementSettingsModel();
        SettingsModelBoolean convertColumnsToStringbooleanSettings = FastExportNodeModel.createAllColumnsAreStringsSettingsModel();
        //SettingsModelBoolean exportToCSVbooleanSettings = FastExportNodeModel.createExportToCSVSettingsModel();
		//SettingsModelString pathToCSVFileStringSettings = FastExportNodeModel.createPathToCSVFileSettingsModel();
        
        // Set if the output will be a CSV file
        //addDialogComponent(new DialogComponentBoolean(exportToCSVbooleanSettings, "Export to CSV: "));
        
        // Set the path to the exported CSV file
        //addDialogComponent(new DialogComponentFileChooser(pathToCSVFileStringSettings, "Exported file path:"));
        
        
        // Set if columns will be all Strings or not
        addDialogComponent(new DialogComponentBoolean(convertColumnsToStringbooleanSettings, "Convert all columns to String: "));
		
		// Set Bulk Size
		addDialogComponent(new DialogComponentNumber(integerSettings, "Bulk size:", 1000000));
		
		// Set select statement
		addDialogComponent(new DialogComponentMultiLineString(stringSettings, "Select Statement:"));
        
    }
}

