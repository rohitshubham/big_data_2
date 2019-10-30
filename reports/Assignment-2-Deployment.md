# Deployment 2

### CS-E4640 Big data platforms 
#### Rohit Raj - 801636
---

This assignment has 4 components that we need to deploy:
1. Mysimbdp-DataBroker
2. Mysimbdp-fetchData
3. Mysimbdp-BatchingIngestManager
4. Mysimbdp-StreamIngestManager


We will deploy the assignment in the same serial as above. Also, you will need root access to install components and run docker. All the docker images are by default pulled from `docker-hub` and run on `tag:latest`.

Before that run the following command in folder `code/script`

```bash
$ pip install -r requirements.txt
```
This will install all the python dependencies.

### 1. Mysimbdp-DataBroker

Since the broker forms the backbone of our notification and reporting service, we need to start this first. To start it, run the shell file `code/mysimbdp-databroker/mysimbdp-databroker.sh` with root privileges. This will install docker, fetch `eclipse-moquitto` docker image and start it on port 1883 for TCP connections.  

### 2. Mysimbdp-fetchData

To start this component, run the python file `code/mysimbdp-fetchdata/mysimbdp-fetchdata.py`. You can edit the config.json file to see the changes.

### 3. Mysimbdp-BatchingIngestManager

To run this component, run the code `code/mysimbdp-batchingestmanager/mysimbdp-batchingestmanager.py`.  The batching ingest manager automatically takes care of invoking client batch ingest scripts.

### 4. Mysimbdp-StreamIngestManager and reporting service

To run this component, run the code `code/mysimbdp-streamingestmanager/reporting_service.py`.

To  manually invoke the first instance of client 1 use the command :

```bash
$ sudo python3 mysimbdp-streamingestmanager.py client1 start 0
```
To manually invoke first instance of client 2's stream ingest use the command:

```bash
$ sudo python3 mysimbdp-streamingestmanager.py client2 start 0
```

The reporting service will now automatically create and remove instance of both these scripts depending on report data.

---

To simulate the sending the data in the `ingestMessageStructure`, use the code in the folder: `code/script/Company_Number_ingestmessagestructure.py` 

---

To run the old `mysimbdp-coredms`, run the code :
1. Install `docker` and `docker-compose` using the script `/code/scripts/install_docker.sh`
2. Run the script `/code/scripts/python-dependencies.sh`
2. Run the script `/code/scripts/install_mongo_container.sh`. This will automatically pull, install, enable replication and shard the database named `mysimbdp-coredms` and run all the 10 containers.  

To access the terminal interface of our sharded mongoDB use the command:
```bash
sudo docker-compose exec -i router mongo 
```

To check the status of sharding use 
> sh.status()

in the terminal.

To stop the database, just use 
```bash
sudo docker-compose stop
```
