# ADAM Big Data Architecture Simulation

 A Big Data architecture simulation using PostgreSQL, Debezium, Kafka, Spark, MinIO, Elasticsearch, Backend, and Frontend.

 ## Demo
 Click to see on YouTube:
 [![Demo Sim YouTube](https://img.youtube.com/vi/RomImGPMCAo/maxresdefault.jpg)](https://youtu.be/RomImGPMCAo)

 ## Requirements

 - Git
- Docker
- Docker Compose

 ## Quick Start

 ### 1\. Clone the repository

```
git clone https://github.com/NotMyTata/ADAM-BigData-Architecture-Sim.git
cd ADAM-BigData-Architecture-Sim
```

 ### 2\. Start the application

```
docker compose up --build -d
```

 ### 3\. Check the services
All services should be active

```
docker compose ps
```

### 5\. Web UI
Open frontend/index.html

 ## Services

 | Service | Port |
| --- | --- |
| PostgreSQL | `5432` |
| Backend | `8000` |
| Debezium | `8083` |
| MinIO API | `9000` |
| MinIO Console | `9001` |
| Kafka | `9092` |
| Elasticsearch | `9200` |

## Access

 - Backend: http://localhost:8000
- Debezium: http://localhost:8083
- MinIO Console: http://localhost:9001
- Elasticsearch: http://localhost:9200

 ### MinIO Credentials

```
Username: minioadmin
Password: minioadmin
```

 ## Stop

```
docker compose down
```

 To completely reset the environment:

```
docker compose down -v
```

 > `docker compose down -v` removes the project's persistent Docker volumes and stored data.
