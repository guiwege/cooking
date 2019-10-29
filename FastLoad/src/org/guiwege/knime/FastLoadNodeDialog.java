package org.guiwege.knime;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/** 
 * @author Guilherme Wege Chagas
 */
public class FastLoadNodeDialog extends DefaultNodeSettingsPane {

    protected FastLoadNodeDialog() {
        super();

        SettingsModelInteger integerSettings = FastLoadNodeModel.createBulkSizeSettingsModel();
		SettingsModelString stringSettings = FastLoadNodeModel.createTableNameSettingsModel();
		
        addDialogComponent(new DialogComponentNumber(integerSettings, "Bulk size:", 1000000));
		addDialogComponent(new DialogComponentString(stringSettings, "Table name:", true, 24));		
    }
}

