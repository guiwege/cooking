Hey!

This project is for creating two KNIME nodes for working with Teradata.

FastExport Node: Exports data from Teradata into a KNIME table using Teradata's FastExport utility. 
FastLoad Node: Loads data from a KNIME table into a Teradata empty table using Teradata's FastLoad utility.

My initial idea was to share them from KNIME's repositories so that everyone would be able to download them directly from KNIME, 
but apparently, this is not legally permitted, because the package would contain terajdbc4.jar, which is a driver owned by Teradata.
I tried to reach them to ask for information about this, but had no success.

So, we are gonna have to go the "hard" way with this, which is not really that hard.

1. Follow this guide for setting up Eclipse for building custom KNIME nodes: https://www.knime.com/developer-guide

2. Import the project into Eclipse, create a lib folder and put terajdbc4.jar in it, like this:
org.guiwege.knime.teradata/lib/terajdbc4.jar

The lib folder should be at the same level as the src folder.

3. Right click terajdbc4.jar from the Project Explorer and add it to the build path.


With this you should be able to run KNIME directly from Eclipse and test out the nodes, then you could build them into separate
jar files and include them in your KNIME installation. (Please, do not share this final jar file without Teradata's permission, this is what I *think* mey be illegal).

One more thing, be sure to add "TYPE=FASTEXPORT" or "TYPE=FASTLOAD" to your JDBC connection string. And currently, the nodes will work only with the Database Connection (legacy) node.

I've worked with this code for a few months and it really helped me, it's really fast!
I hope it helps.
