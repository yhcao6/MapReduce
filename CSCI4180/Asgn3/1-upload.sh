# rm mydedup.index

javac -cp .:lib/* AzureStorage.java Index.java MyDedup.java && 

java -cp .:./lib/* MyDedup upload 4 13 100 10 1.txt azure
# java -cp .:./lib/* MyDedup upload 400 512 1000 257 1.png azure

rm *.class
