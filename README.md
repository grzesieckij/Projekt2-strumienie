# Projekt 2 
## Zestaw number 3: Crimes-in-Chicago z wykorzystaniem Kafka Streams

### Informacje ogólne

### Inicjalizacja
w termninalu Google Cloud'a wklej poniżyszy skryt w celu utworzenia clastra:
```
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-2 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components ZEPPELIN,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=2h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```

### Producer


