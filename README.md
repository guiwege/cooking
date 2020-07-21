Hey!

This project is for creating two KNIME nodes for the Teradata datawarehouse.

FastExport Node: Exports data from Teradata into a KNIME table using Teradata's FastExport jdbc utility.
FastLoad Node: Loads data from a KNIME table into a Teradata empty table using Teradata's FastLoad utility.

My initial idea was to share them from KNIME's repositories so that everyone would be able to download them directly from KNIME, 
but apparently, this is not legally permitted, because the package would contain terajdbc4.jar, which is a driver owned by Teradata.
I tried to reach them to ask for information about this, but had no success.

So, we are gonna have to go the "hard" with this, which is really not that hard.
Please, follow this guide for setting up Eclipse for building custom KNIME nodes: https://www.knime.com/developer-guide
Then import my project into Eclipse, create a lib folder and put terajdbc4.jar in it, like this:
org.guiwege.knime.teradata/lib/terajdbc4.jar

The lib folder should be at the same level as the src folder.

Then right click terajdbc4.jar from the Project Explorer and add it to the build path.
With this you should be able to run KNIME directly from Eclipse and test out the nodes, then you could build them into separate
jar files to include in your KNIME installation. (Do not share this final jar file, this is what I *think* is illegal).

I've worked with this code for a few months and it really helped me. I hope it suits your needs.
