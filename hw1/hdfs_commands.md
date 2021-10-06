## См. флаги “-mkdir” и “-touchz“

1. [2 балла] Создайте папку в корневой HDFS-папке
```bash
root@1d51beb2e44c:/# hdfs dfs -mkdir /hw1
```
2. [2 балла] Создайте в созданной папке новую вложенную папку.
```bash
root@1d51beb2e44c:/# hdfs dfs -mkdir /hw1/subdir
```
3. [3 балла] Что такое Trash в  распределенной FS? Как сделать так, чтобы файлы удалялись сразу, минуя  “Trash”?
> По умолчанию при удалении файлов, они не стираются с диска, а переносятся в папку .trash. Это делается на случай нечаянного удаления файла. Количеcтво времени, через которое удалится файл прописывается в конфиге (по умолчанию 4 дня). Чтобы удалить файл, минуя .trash можно прописать флаг -skipTrash в команде hdfs dfs -rm.
4. [2 балла] Создайте пустой файл в подпапке из пункта 2.
```bash
root@1d51beb2e44c:/# hdfs dfs -touchz /hw1/subdir/empty.file
```
5. [2 балла] Удалите  созданный файл
```bash
root@1d51beb2e44c:/# hdfs dfs -rm -skipTrash /hw1/subdir/empty.file
Deleted /hw1/subdir/empty.file
```
6. [2 балла] Удалите  созданные папки.
```bash
root@1d51beb2e44c:/# hdfs dfs -rm -r /hw1
Deleted /hw1
```

## См. флаги “-put”, “-cat”, “-tail”, “-cp”

1. [3 балла] Скопируйте любой в новую папку на HDFS
```bash
root: docker cp ./Kafka_Prevrashchenie.fb2 namenode:/ 
root@1d51beb2e44c:/# hdfs dfs -put Kafka_Prevrashchenie.fb2 /new_folder/kafka.fb2
```
2. [3 балла] Выведите содержимое  HDFS-файла на экран.
```bash
root@1d51beb2e44c:/# hdfs dfs -cat /new_folder/kafka.fb2
```
3. [3 балла] Выведите  содержимое нескольких последних строчек HDFS-файла на экран.
```bash
root@1d51beb2e44c:/# hdfs dfs -tail /new_folder/kafka.fb2
root@1d51beb2e44c:/# hdfs dfs -cat /new_folder/kafka.fb2 | tail -n 10
```
4. [3 балла] Выведите  содержимое нескольких первых строчек HDFS-файла на экран.
```bash
root@1d51beb2e44c:/# hdfs dfs -head /new_folder/kafka.fb2
root@1d51beb2e44c:/# hdfs dfs -cat /new_folder/kafka.fb2 | head -n 10
```
5. [3 балла] Переместите копию файла в HDFS на новую локацию.
```bash
root@1d51beb2e44c:/# hdfs dfs -cp /new_folder/kafka.fb2 /second_folder/pushkin.fb2
```

## См. флаги “--setrep -w” "fsck"
2. [4  баллов]  Изменить replication  factor  для  файла.  Как  долго  занимает  время  на  увеличение / уменьшение числа реплик для файла?
```bash
time hdfs dfs -setrep -w 1 /second_folder/pushkin.fb2
time hdfs dfs -setrep -w 3 /second_folder/pushkin.fb2
```
> Уменьшение заняло 12.288s, увеличение - 11.992s. Не особо существенная разница, но при уменьшении к тому же вывелось сообщение "WARNING: the waiting time may be long for DECREASING the number of replications.

3. [4 баллов] Найдите  информацию по файлу, блокам и их расположениям с помощью “hdfs fsck”
```bash
root@1d51beb2e44c:/# hdfs fsck /new_folder/ -files -blocks -locations
```
4. [4  баллов]  Получите    информацию  по  любому  блоку  из  п.2  с  помощью  "hdfs  fsck  -blockId”. Обратите внимание на Generation Stamp (GS number).
```bash
root@1d51beb2e44c:/# hdfs fsck -blockId blk_1073741836
```