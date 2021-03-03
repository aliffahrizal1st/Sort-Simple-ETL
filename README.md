# Sort-Simple-ETL
# Daily Hotel Booking Funnel

*This project was made to accomplish Pegipegi Data Engineer Internship Techinal Test.

In this project, I used 2 (two) library for creating an simple ETL (Extract, Tranfrom, Load) Pipeline. The library is Pyspark and Pandas. For Pyspark I intend to install Spark first to create a container, and then install Pyspark library so when the time I need to use Spark in Python, I don't need to open Spark. Maybe you can still find the step at [this site](https://towardsdatascience.com/how-to-get-started-with-pyspark-1adc142456ec) and [another site](https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421).

For the program it self, I create a session so i can work with the data as a table, especially Pyspark can Transform the data using SQL system. Then after that, I Extract the data or more like read the data then put it value in 'dataset' variable. After that I must check the data like the number of rows, and the schema. But since working with Pyspark means every you read a dataset as a table, the data type on that table is always String. So, to do that I tried to take the data when doing Transform and declare it as a new table. After that, I checked on the schema again it written that some of columns don't have the data type that needed. And that, I change the data type all the 14 column so it match to the terms. After changing the data type, I Load it to CSV file.

Thank you for reading my documentation, i hope this help you to read my code. 

Note :
I'm apologize if my explanation or my code is very confusing, because I recently started Data Engineer at last semester and it's been a while i'm not using database
