# libskeen
Skeen algorithm, totally ordered atomic multicast primitive

Compile:\
Let's say PathToLibskeen is the folder containing the libskeen

    cd PathToLibskeen
    mvn compile
    mvn dependency:copy-dependencies

Run:  
To run a server:

    java -Dlog4j.configuration=file:PathToLibskeen/bin/log4jDebug.xml -cp PathToLibskeen/target/classes:PathToLibskeen/target/dependency/* ch.usi.dslab.mojtaba.libskeen.Server 0 PathToLibskeen/bin/skeen_system_config.json
    
In the example config files there are two servers. You need to run the servers with IDs 0 and 1

To run a client:

    java -Dlog4j.configuration=file:PathToLibskeen/bin/log4jDebug.xml -cp PathToLibskeen/target/classes:PathToLibskeen/target/dependency/* ch.usi.dslab.mojtaba.libskeen.Client 100 PathToLibskeen/bin/skeen_system_config.json

Clients IDs need to be different from IDs of the servers. The above example gives ID 1000 to the client.
