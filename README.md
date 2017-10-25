## IndexHashMap.java
### General
This class implement the inverted index and is used to read .csv data files and get the corresponding index of the term.
### Usage
Spcifiy the path of input data, set the `ExecutionEnvironment`. Note that this input path is also used for output:
```java
String input = "you/data/path";
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
```
initiate the Index:
```java
IndexHashmap Index = new IndexHashmap(env, input);
```
You can easily get the index by:
```java
Index.getIndex();
```
To get access to or print the indices containing IDF and TF, use method `getIDF()` and `getTF()`:
```java
Index.getIDF();
Index.IDF.print();

Index.getTF();
Index.TF.print();
```
Write to .csv files:
```java
Index.writeIndex()
```
read IDF and TF indices:
```java
// <word, idf>
DataSet<Tuple2<String, Double>> inputIDF = Index.readIDF();

// <word, docID, docLength, tf>
DataSet<Tuple4<String, String, Integer, Double>> inputTF = Index.readTF();
```

## BMScore.java
### General
This class implement the BM25 ranking algorithm with an input of query string and outputs a list of results
### Usage
initialize the BMScore object along with the query string
```java
String query = "python is the best programming language";
BMScore bm25 = new BMScore(query);
```
get the output list of ranking results
```java
List<Tuple1<String>> ranking = bm25.computeScore();
```

## Module: Server
### General
This module is the server part for data process. The module contains four classes.
* __DBConnection.java__
<br/>This class realize the connection to the database, and initial PreparedStatement.
* __Result.java__
<br/>This is an entity, contains three attributes title(title of question), url(url of official webpage) and preview(body of question).
* __Service.java__
<br/>This class implement the query process, call the function in Flink part and get a rank page list, then find the information from database according to list, the found  information will be packaged as json return to the  webpage.
* __StartEntity.java__
<br/>In this class we will start the server.

