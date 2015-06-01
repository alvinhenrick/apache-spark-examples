# Apache Spark Examples

## Basic Map Function

## Basic Average with Aggregate Function

## WordCount Example -- No Dependencies -- Assembly not required.
```sh
spark-submit \
--class com.wordcount.example.WordCount \
--driver-memory "3g" \
target/scala-2.10/dfw-spark-meetup_2.10-0.0.1.jar \
/Users/shona/IdeaProjects/apache-spark-examples/data/pg4300.txt \
/Users/shona/IdeaProjects/apache-spark-examples/data/wordcount
```
## Top 5 User Count Stackoverflow -- No Dependencies -- Assembly not required.
```sh
spark-submit \
--class com.stackoverflow.example.UserCount \
--master "local[1]" \
--driver-memory "3g" \
target/scala-2.10/dfw-spark-meetup_2.10-0.0.1.jar \
/Users/shona/IdeaProjects/apache-spark-examples/data/Users.xml
```

## Naive Bayes Tweet Sentimental Analysis -- Lucene Text Analyzer -- Assembly required.
```sh
spark-submit \
--class com.twitter.example.NaiveBayesClassifier \
--driver-memory "5g" \
target/scala-2.10/dfw-spark-meetup-assembly-0.0.1.jar
```

## Twitter Streaming -- Apache Spark Example -- Printing on Console
```sh
spark-submit \
--class org.apache.spark.examples.streaming.TwitterPopularTags \
--master "local[2]"  \
--driver-memory "3g" \
lib/spark-examples-1.3.1-hadoop2.6.0.jar \
```

## Explain and Discuss Aggregate Function
```sh
val numbers = sc.parallelize(List(1,2,3,4,5,6), 2)
numbers.aggregate(0)(math.max(_, _), _ + _)
```
## Explain and Discuss RDD and toDebugString
```sh
val file = sc.textFile("README.md")
val containsSpark = file.filter(line => line.contains("Spark"))
containsSpark foreach println
val words = containsSpark.flatMap(line => line.split(" "))
val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
println output
```
## Explain and Discuss Accumulator
```sh
val accum = sc.accumulator(0, "Test Accumulator")
sc.parallelize(Array(7,8, 9, 10)).foreach(x => accum += x)
```
## Explain and Discuss Broadcast
```sh
val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar.value
```

## Sample Data Download Links
* [Stack Exchange] - Stackoverflow data download.
* [Sample Text File] - Sample Large Text file download.
* [Sentiment Analysis Dataset] - Sample Tweets for training the Naive Bayes model.


[Stack Exchange]:https://archive.org/details/stackexchange
[Sample Text File]:http://www.gutenberg.org/ebooks/4300
[Sentiment Analysis Dataset]:http://thinknook.com/wp-content/uploads/2012/09/Sentiment-Analysis-Dataset.zip