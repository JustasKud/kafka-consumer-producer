# Requirements

In order to run this project, a few programs must be installed:

- Docker
- Python >=3.9

I will assume everything is run on Windows 10.

# Installing dependencies

After cloning the project, make sure to create a virtual environemtn with your favorite tool. We will use `virtualenv`. Go to the cloned directory and execute these commands:

```bash
pip install virtualenv
python -m virtualenv venv
source ./venv/Scripts/activate
pip install -r requirements.txt
```

# Start Kafka

Open up Docker dashboard and then from the project root run:

```bash
docker-compose up -d
```

Now Kafka's endpoints are exposed at localhost:29092.

# Start scripts

Open up 4 terminals from the root of the project folder and run these scripts:

- admin.py to create topics
- consumer.py to consume deduplicated messages
- deduplicator.py to consume raw messages and send deduplicated messages
- producer.py to produce raw messages from dataset

using this command:

```bash
python [script].py
```

In addition, consumer.py accepts integer argument for number of rows to read. By default it is 500 rows. If you want 1000 rows, you can write:

```bash
python producer.py 1000
```
