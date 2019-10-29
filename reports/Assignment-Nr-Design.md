# Assignment report 2

### CS-E4640 Big data platforms 
#### Rohit Raj ([rohit.raj@aalto.fi](mailto:rohit.raj@aalto.fi)) - 801636
---
## Part 1
### Design of the project

The project has basically two ingestion methods:

* __Stream Ingestion__
* __Batch Ingestion__

Each of these ingestion methods operate separately and independently. However, the data sink is same for both these systems. That is, the data/file is finally stored inside the sharded `Mongodb` instance (mysimbdp-coredms) designed in the previous assignment.

The different components and data flow design of the system is illustrated in the following diagram:

![Ingestion_Diagram](./images/BDP_2.png)
* Fig 1: Ingestion Architecture

### Batch Ingestion:

The batch ingestion has following components:
* __mysimbdp-fetchdata__
* __Batch Ingest Manager__
* __Client Batching Ingest App__

The batching ingest app is responsible for copying the file into the system.

### Mysimbdp-FetchData

The role of this component is to move the data from the `client-input-directory` to staging destination. It keeps on constantly checking the directory for any existence of any file. 

Whenever a file is detected in the folder, `Fetch-data` loads the __*`config.json`*__ file. The `config.json` file has two types of validation/ configuration 
1. Generic Configuration : These include general validation tests that all files have to pass irrespective of the client. We can say that this is enforced by the mysimbdp. Some of the examples are max file size limit should not exceed 100Mb, or the filename should not be bigger than 20 characters.  

2. Client-specific configurations: This configuration data is specific for every client. Every client has a unique clientID which can be used to uniquely identify the system. For example: Client 1 has turned on micro-batching based on incoming file size. Moreover, it has also defined the block length to be 10Mb. We also have other client specific checks like accepted data formats etc. 

The `fetchData` component actively first validates the generic configuration checks and then proceeds to check the client specific check. 


