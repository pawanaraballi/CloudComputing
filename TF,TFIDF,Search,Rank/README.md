# CloudComputing
The consists of 5 documents DocWordCount.java, TermFrequency.java, TFIDF.java, search.java and rank.java
Note: Every time when running a new program please make sure all the folder structure has been deleted. 


<b>DocWordCount.java:</b> This program basically is an extension of the WordCount where we append a delimiter and the filename from where the word is being fetched.
Execution Instructions:
<ul>
<li>Create the folder structure required Hadoop fs -mkdir /user/cloudera/docwordcount /user/cloudera/docwordcount/input </li>
<li>Insert the data into the Hadoop filesystem Hadoop fs –put file* /user/cloudera/docwordcount/input </li>
<li>Compile the file javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint </li>
<li>Create the jar file jar -cvf docwordcount.jar -C build/ . </li>
<li>Run the hadoop hadoop jar docwordcount.jar DocWordCount /user/cloudera/doccwordcount/input /user/cloudera/docwordcount/output </li> 
<li>The output file would be viewed in Hadoop fs -cat /user/cloudera/docwordcount/output/* </li>
</ul>

<b>Sample Output </b>

Hadoop&#&#&file1.txt   2<br>
Hadoop&#&#&file2.txt		1<br>
an&#&#&file2.txt		1<br>
elephant&#&#&file2.txt		1<br>
is&#&#&file1.txt		1<br>
is&#&#&file2.txt		1<br>
yellow&#&#&file1.txt		1<br>
yellow&#&#&file2.txt		1<br>


<b>TermFrequency.java:</b> This program is an extension of the DocWordCount where we calculate the termfrequency of each word.
<ul>
<li>Create the folder structure required Hadoop fs -mkdir /user/cloudera/termfrequency /user/cloudera/termfrequency/input </li>
<li>Insert the data into the Hadoop filesystem Hadoop fs –put file* /user/cloudera/termfrequency/input </li>
<li>Compile the file javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint </li>
<li>Create the jar file jar -cvf termfrequency.jar -C build/. </li>
<li>Run the hadoop hadoop jar termfrequency.jar TermFrequency /user/cloudera/termfrequency/input /user/cloudera/termfrequency/output </li> 
<li>The output file would be viewed in Hadoop fs -cat /user/cloudera/ termfrequency/output/* </li>
</ul>

<b>Sample Output </b>

Hadoop&#&#&file1.txt		1.30103<br>
Hadoop&#&#&file2.txt		1.0<br>
an&#&#&file2.txt		1.0<br>
elephant&#&#&file2.txt		1.0<br>
is&#&#&file1.txt		1.0<br>
is&#&#&file2.txt		1.0<br>
yellow&#&#&file1.txt		1.0<br>
yellow&#&#&file2.txt		1.0<br>

<b>TFIDF.java:</b> This program is an extension of TermFrequency where we calculate the termfrequency and use the output of that value to calculate the tfidf value. Here I have created a temporary location to store the value of the termfrequency value 

<ul>
<li>Create the folder structure required Hadoop fs -mkdir /user/cloudera/tfidf /user/cloudera/tfidf/input </li>
<li>Insert the data into the Hadoop filesystem Hadoop fs –put file* /user/cloudera/tfidf/input </li>
<li>Compile the file javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint </li>
<li>Create the jar file jar -cvf tfidf.jar -C build/. </li>
<li>Run the hadoop hadoop jar tfidf.jar TFIDF /user/cloudera/tfidf/input /user/cloudera/tfidf/output </li>
<li>The output file would be viewed in Hadoop fs -cat /user/cloudera/tfidf/output/* </li>
</ul>

<b>Sample Output </b>

Hadoop&#&#&file2.txt	0.3010299956639812 <br>
Hadoop&#&#&file1.txt	0.3916490552587094 <br>
an&#&#&file2.txt	0.47712125471966244 <br>
elephant&#&#&file2.txt	0.47712125471966244 <br>
is&#&#&file2.txt	0.3010299956639812 <br>
is&#&#&file1.txt	0.3010299956639812 <br>
yellow&#&#&file2.txt	0.3010299956639812 <br>
yellow&#&#&file1.txt	0.3010299956639812 <br>


<b>Search.java:</b> This program is extension of TFIDF where we search for words in the file and output the tfidf value for a particular file containing the word. In this file again I am creating the intermediate folders for storing the termfrequency and tfidf.
<ul>
<li>Create the folder structure required Hadoop fs -mkdir /user/cloudera/search /user/cloudera/search/input </li>
<li>Insert the data into the Hadoop filesystem Hadoop fs –put file* /user/cloudera/search/input </li>
<li>Compile the file javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* search.java -d build -Xlint </li>
<li>Create the jar file jar -cvf search.jar -C build/. </li>
<li>Run the hadoop hadoop jar search.jar search /user/cloudera/search/input /user/cloudera/search/output </li> 
<li>The output file would be viewed in Hadoop fs -cat /user/cloudera/search/output/* </li>
</ul>

<b>Sample Output </b>

file2.txt	0.60206 <br>
file1.txt	0.69267905 <br>

<b>Rank.java:</b> This program is extension of Search where we sort the value to get the best fit the searched word. In this file again I am creating the intermediate folders for storing the termfrequency, tfidf and search.
<ul>
<li>Create the folder structure required Hadoop fs -mkdir /user/cloudera/rank /user/cloudera/rank/input </li>
<li>Insert the data into the Hadoop filesystem Hadoop fs –put file* /user/rank/search/input </li>
<li>Compile the file javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* rank.java -d build -Xlint </li>
<li>Create the jar file jar -cvf rank.jar -C build/. </li>
<li>Run the hadoop hadoop jar rank.jar search /user/cloudera/rank/input /user/cloudera/rank/output </li>
<li>The output file would be viewed in Hadoop fs -cat /user/cloudera/rank/output/* </li>
</ul>

<b>Sample Output </b>

file1.txt	0.69267905 <br>
file2.txt	0.60206 <br>
