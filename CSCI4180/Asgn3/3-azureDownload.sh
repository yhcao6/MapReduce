# rm mydedup.index

javac -cp .:lib/* AzureStorage.java Index.java MyDedup.java && 

# java -cp .:./lib/* MyDedup download ./1.txt azure && mv 1.txt.download 2.txt
java -cp .:./lib/* MyDedup download ./1.png azure && mv 1.png.download 2.png

rm *.class
