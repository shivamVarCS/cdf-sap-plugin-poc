# sap-plugins
Multi module SAP plugins project

Modules
-------

1. sap-table-plugins
2. sap-bwreader-plugin   
3. sap-delta-plugins


Setup
-----
After checking out the project, import the SAP JCo dependencies ``sapjco3.jar``
and platform dependent system file, like ``libsapjco3.so`` for Unix, ``libspjco3.dll`` for Windows etc. in this ``lib`` folder.


Build
-----
To build plugin:

    mvn clean package -DskipTests

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json and .jar file under the ``target`` directory.

Ensure the generated files' names are same and only their extensions differ.
These files can be used to deploy the plugin.
