
Using official Flink 1.11.1 docker image to play with the stream applications of this project. After building the docker image and startup it access the Flink WebUI of Flink cluster at [http://localhost:8081](http://localhost:8081).

```
cd operations-playground
docker-compose build
docker-compose up -d
docker-compose down
```

