# rm mydedup.index

javac -cp .:lib/* AzureStorage.java Index.java MyDedup.java && 

java -cp .:./lib/* MyDedup delete 1.txt azure
# java -cp .:./lib/* MyDedup delete ./1.png azure

rm *.class
